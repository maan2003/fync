use anyhow::Result;
use iroh_blake3::Hash as ContentHash;
use notify_debouncer_full::{
    new_debouncer,
    notify::{RecommendedWatcher, RecursiveMode, Watcher},
    DebouncedEvent, Debouncer, FileIdMap,
};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

#[derive(Debug, Eq, PartialOrd, Ord, PartialEq, Clone)]
pub struct FilePath(Arc<str>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetadata {
    content_hash: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsState {
    files: BTreeMap<FilePath, FileMetadata>,
}

impl FsState {
    pub fn from_disk(root: &Path) -> Result<Self> {
        let mut files = BTreeMap::new();
        for entry in ignore::Walk::new(root) {
            let entry = entry?;
            if entry.file_type().map_or(false, |d| d.is_file()) {
                let path = entry.path();
                let mut file = std::fs::File::open(path)?;
                let mut hasher = iroh_blake3::Hasher::new();
                std::io::copy(&mut file, &mut hasher)?;
                let content_hash = hasher.finalize();
                let file_name = FilePath(Arc::from(
                    path.strip_prefix(root)?.to_string_lossy().as_ref(),
                ));
                let metadata = FileMetadata { content_hash };
                files.insert(file_name, metadata);
            }
        }
        Ok(FsState { files })
    }

    pub fn refresh_path(&mut self, root: &Path, file: &Path) -> Result<()> {
        let relative_path = file.strip_prefix(root)?;
        let file_name = FilePath(Arc::from(relative_path.to_string_lossy().as_ref()));

        if file.is_file() {
            let mut file_handle = std::fs::File::open(file)?;
            let mut hasher = iroh_blake3::Hasher::new();
            std::io::copy(&mut file_handle, &mut hasher)?;
            let content_hash = hasher.finalize();

            let metadata = FileMetadata { content_hash };

            self.files.insert(file_name, metadata);
        } else {
            self.files.remove(&file_name);
        }

        Ok(())
    }

    pub fn refresh_paths(&mut self, root: &Path, paths: &[PathBuf]) -> Result<()> {
        for path in paths {
            self.refresh_path(root, path)?;
        }
        Ok(())
    }

    pub fn diff(&self, next: &Self) -> FsStateDiff {
        let mut files = BTreeMap::new();

        for (file_name, metadata) in &self.files {
            match next.files.get(file_name) {
                Some(other_metadata) => {
                    if metadata.content_hash != other_metadata.content_hash {
                        files.insert(
                            file_name.clone(),
                            FileChange::Modified {
                                old_meta: metadata.clone(),
                                new_meta: other_metadata.clone(),
                            },
                        );
                    }
                }
                None => {
                    files.insert(
                        file_name.clone(),
                        FileChange::Removed {
                            old_meta: metadata.clone(),
                        },
                    );
                }
            }
        }

        for (file_name, metadata) in &next.files {
            if !self.files.contains_key(file_name) {
                files.insert(
                    file_name.clone(),
                    FileChange::Created {
                        meta: metadata.clone(),
                    },
                );
            }
        }

        FsStateDiff { files }
    }

    pub fn check_diff(&self, diff: &FsStateDiff) -> (Vec<PathBuf>, FsStateDiff) {
        let mut conflicts = Vec::new();
        let mut unconflicted_diff = FsStateDiff {
            files: BTreeMap::new(),
        };

        for (file_name, change) in &diff.files {
            let is_conflict = match change {
                FileChange::Removed { old_meta } => {
                    if let Some(current_meta) = self.files.get(file_name) {
                        current_meta.content_hash != old_meta.content_hash
                    } else {
                        false
                    }
                }
                FileChange::Created { meta } => {
                    if let Some(current_meta) = self.files.get(file_name) {
                        current_meta.content_hash != meta.content_hash
                    } else {
                        false
                    }
                }
                FileChange::Modified { old_meta, new_meta } => {
                    if let Some(current_meta) = self.files.get(file_name) {
                        current_meta.content_hash != old_meta.content_hash
                            && current_meta.content_hash != new_meta.content_hash
                    } else {
                        true
                    }
                }
            };

            if is_conflict {
                conflicts.push(PathBuf::from(file_name.0.as_ref()));
            } else {
                unconflicted_diff
                    .files
                    .insert(file_name.clone(), change.clone());
            }
        }

        (conflicts, unconflicted_diff)
    }
}

#[derive(Debug)]
pub struct FsStateDiff {
    // TODO: avoid sending same file path again and again
    files: BTreeMap<FilePath, FileChange>,
}

#[derive(Debug, Clone)]
enum FileChange {
    Removed {
        old_meta: FileMetadata,
    },
    Created {
        meta: FileMetadata,
    },
    Modified {
        old_meta: FileMetadata,
        new_meta: FileMetadata,
    },
}

impl FsStateDiff {
    pub fn apply(&self, state: &mut FsState) {
        for (file_name, change) in &self.files {
            match change {
                FileChange::Removed { .. } => {
                    state.files.remove(file_name);
                }
                FileChange::Created { meta } => {
                    state.files.insert(file_name.clone(), meta.clone());
                }
                FileChange::Modified { new_meta, .. } => {
                    if let Some(existing_meta) = state.files.get_mut(file_name) {
                        *existing_meta = new_meta.clone();
                    }
                }
            }
        }
    }

    pub fn apply_to_disk(&self, root: &Path, content_store: &ContentStore) -> Result<()> {
        for (file_name, change) in &self.files {
            let full_path = root.join(file_name.0.as_ref());
            match change {
                FileChange::Removed { .. } => {
                    std::fs::remove_file(&full_path)?;
                }
                FileChange::Created { meta } => {
                    if let Some(parent) = full_path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    let content = content_store.get(&meta.content_hash)?;
                    std::fs::write(&full_path, content)?;
                }
                FileChange::Modified { new_meta, .. } => {
                    let content = content_store.get(&new_meta.content_hash)?;
                    std::fs::write(&full_path, content)?;
                }
            }
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

#[derive(Default, Debug)]
pub struct ContentStore {
    // todo: use better hash map
    contents: HashMap<ContentHash, Vec<u8>>,
}

impl ContentStore {
    pub fn add(&mut self, content: Vec<u8>) -> ContentHash {
        let hash = iroh_blake3::hash(&content);
        self.insert(hash, content);
        hash
    }

    pub fn insert(&mut self, hash: ContentHash, content: Vec<u8>) {
        self.contents.insert(hash, content);
    }

    pub fn get(&self, hash: &ContentHash) -> Result<&[u8]> {
        self.contents
            .get(hash)
            .ok_or_else(|| anyhow::anyhow!("Content not found in store"))
            .map(|x| x.as_slice())
    }

    pub fn remove(&mut self, hash: &ContentHash) {
        self.contents.remove(hash);
    }

    pub fn has(&self, hash: &ContentHash) -> bool {
        self.contents.contains_key(hash)
    }
}

pub struct Node {
    this_state: FsState,
    last_transmitted_state: FsState,
    other_state: FsState,
    content_store: ContentStore,
    conflicts: Vec<PathBuf>,
}

impl Node {
    pub fn new(this_state: FsState, other_state: FsState, content_store: ContentStore) -> Self {
        Self {
            this_state,
            last_transmitted_state: other_state.clone(),
            other_state,
            content_store,
            conflicts: Vec::new(),
        }
    }

    pub fn changes_for_other(&mut self) -> FsStateDiff {
        let diff = self.last_transmitted_state.diff(&self.this_state);
        self.last_transmitted_state = self.this_state.clone();
        diff
    }

    pub fn changes_acked_by_other(&mut self, diff: &FsStateDiff) {
        let (conflicts, unconflicted_diff) = self.other_state.check_diff(diff);
        if !conflicts.is_empty() {
            eprintln!("Warning: Unexpected conflicts in acked changes");
        }
        unconflicted_diff.apply(&mut self.other_state);
    }

    pub fn apply_changes_from_other(&mut self, diff: &FsStateDiff) -> Result<()> {
        diff.apply(&mut self.other_state);
        let (conflicts, unconflicted_diff) = self.this_state.check_diff(diff);
        self.conflicts.extend(conflicts);
        unconflicted_diff.apply(&mut self.this_state);
        Ok(())
    }

    pub fn has_conflicts(&self) -> bool {
        !self.conflicts.is_empty()
    }

    pub fn is_settle(&self) -> bool {
        self.this_state == self.other_state
    }

    pub fn content_store(&self) -> &ContentStore {
        &self.content_store
    }
}

// TODO: don't watch git ignored paths
pub fn watch_root(
    root: &Path,
    handler: impl Fn(Vec<PathBuf>) + Send + 'static,
) -> Result<Debouncer<RecommendedWatcher, FileIdMap>> {
    let mut debouncer = new_debouncer(
        Duration::from_millis(10),
        None,
        move |result: Result<Vec<DebouncedEvent>, _>| match result {
            Ok(events) => handler(
                events
                    .into_iter()
                    .flat_map(|mut x| std::mem::take(&mut x.paths))
                    .collect(),
            ),
            Err(e) => eprintln!("Error in file watcher: {:?}", e),
        },
    )?;

    // TODO: check how this interacts with new directories
    // FIXME: this wasted effort by walking the tree *once again*
    debouncer.watcher().watch(root, RecursiveMode::Recursive)?;
    Ok(debouncer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    impl FsState {
        fn empty() -> Self {
            FsState {
                files: Default::default(),
            }
        }
        fn insert_file(&mut self, path: &str, content: ContentHash) {
            let file_path = FilePath(Arc::from(path));
            let metadata = FileMetadata {
                content_hash: content,
            };
            self.files.insert(file_path, metadata);
        }
        fn remove_file(&mut self, path: &str) {
            let file_path = FilePath(Arc::from(path));
            self.files.remove(&file_path);
        }
    }
    #[test]
    fn test_fs_state() {
        // Create a new ContentStore and add two content hashes
        let mut cs = ContentStore::default();
        let h1 = cs.add(b"hello world".to_vec());
        let h2 = cs.add(b"bye world".to_vec());

        // Create and populate the first file state
        let mut state1 = FsState::empty();
        state1.insert_file("file1.txt", h1);
        state1.insert_file("file2.txt", h2);

        // Create and populate the second file state
        let mut state2 = FsState::empty();
        state2.insert_file("file1.txt", h1);
        state2.insert_file("file3.txt", h2);

        // Calculate the difference between state1 and state2
        let diff = state1.diff(&state2);

        // Check if the diff contains the expected changes
        assert_eq!(diff.files.len(), 2);
        assert!(matches!(
            diff.files.get(&FilePath(Arc::from("file2.txt"))),
            Some(FileChange::Removed { .. })
        ));
        assert!(matches!(
            diff.files.get(&FilePath(Arc::from("file3.txt"))),
            Some(FileChange::Created { .. })
        ));

        // Apply the diff to state1
        diff.apply(&mut state1);

        assert_eq!(state1, state2);
    }

    #[test]
    fn test_node() {
        let mut cs1 = ContentStore::default();
        let mut cs2 = ContentStore::default();

        let h1 = cs1.add(b"hello world".to_vec());
        let h2 = cs1.add(b"bye world".to_vec());
        cs2.add(b"hello world".to_vec());
        cs2.add(b"bye world".to_vec());

        let mut state1 = FsState::empty();
        state1.insert_file("file1.txt", h1);
        state1.insert_file("file2.txt", h2);

        let mut state2 = FsState::empty();
        state2.insert_file("file1.txt", h1);
        state2.insert_file("file3.txt", h2);

        let mut node1 = Node::new(state1.clone(), state2.clone(), cs1);
        let mut node2 = Node::new(state2.clone(), state1.clone(), cs2);

        assert!(!node1.is_settle());
        assert!(!node2.is_settle());

        // Simulate changes on node1
        let diff1 = node1.changes_for_other();
        node2.apply_changes_from_other(&diff1).unwrap();

        assert!(!node2.has_conflicts());
        assert!(node2.is_settle());

        assert!(!node1.is_settle());
        // Simulate changes on node2
        node1.changes_acked_by_other(&diff1);

        assert!(!node1.has_conflicts());
        assert!(node1.is_settle());
        // Check if both nodes have the same state
        assert_eq!(node1.this_state, node2.this_state);
        assert_eq!(node1.other_state, node2.other_state);
    }
}
