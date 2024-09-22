use anyhow::Result;
use iroh_blake3::Hash as ContentHash;
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::Arc,
    time::SystemTime,
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
                let mtime = entry.metadata()?.modified()?;
                let file_name = FilePath(Arc::from(path.to_string_lossy().as_ref()));
                let metadata = FileMetadata {
                    content_hash,
                    // mtime,
                };
                files.insert(file_name, metadata);
            }
        }
        Ok(FsState { files })
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

    pub fn check_diff(&self, diff: &FsStateDiff) -> bool {
        for (file_name, change) in &diff.files {
            match change {
                FileChange::Removed { old_meta } => {
                    if let Some(current_meta) = self.files.get(file_name) {
                        if current_meta.content_hash != old_meta.content_hash {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                FileChange::Created { .. } => {
                    if self.files.contains_key(file_name) {
                        return false;
                    }
                }
                FileChange::Modified { old_meta, .. } => {
                    if let Some(current_meta) = self.files.get(file_name) {
                        if current_meta.content_hash != old_meta.content_hash {
                            return false;
                        }
                        // TODO: check mtime
                    } else {
                        return false;
                    }
                }
            }
        }
        true
    }
}

#[derive(Debug)]
pub struct FsStateDiff {
    // TODO: avoid sending same file path again and again
    files: BTreeMap<FilePath, FileChange>,
}

#[derive(Debug)]
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
    other_state: FsState,
    content_store: ContentStore,
}

impl Node {
    pub fn changes_for_other(&self) -> FsStateDiff {
        self.other_state.diff(&self.this_state)
    }

    pub fn changes_acked_by_other(&mut self, diff: &FsStateDiff) {
        debug_assert!(self.other_state.check_diff(diff));
        diff.apply(&mut self.other_state);
    }

    pub fn apply_changes_from_other(&mut self, diff: &FsStateDiff) -> Result<()> {
        diff.apply(&mut self.other_state);
        if self.this_state.check_diff(diff) {
            diff.apply(&mut self.this_state);
        } else {
            // TODO: fix later, only stop syncing for conflicted paths
            panic!("conflicts found, exiting");
        }
        Ok(())
    }

    pub fn is_settle(&self) -> bool {
        self.this_state == self.other_state
    }
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
        cs2.insert(h1, b"hello world".to_vec());
        cs2.insert(h2, b"bye world".to_vec());

        let mut state1 = FsState::empty();
        state1.insert_file("file1.txt", h1);
        state1.insert_file("file2.txt", h2);

        let mut state2 = FsState::empty();
        state2.insert_file("file1.txt", h1);
        state2.insert_file("file3.txt", h2);

        let mut node1 = Node {
            this_state: state1.clone(),
            other_state: state2.clone(),
            content_store: cs1,
        };

        let mut node2 = Node {
            this_state: state2.clone(),
            other_state: state1.clone(),
            content_store: cs2,
        };

        assert!(!node1.is_settle());
        assert!(!node2.is_settle());

        // Simulate changes on node1
        let diff1 = node1.changes_for_other();
        node2.apply_changes_from_other(&diff1).unwrap();

        assert!(node2.is_settle());

        assert!(!node1.is_settle());
        // Simulate changes on node2
        node1.changes_acked_by_other(&diff1);

        assert!(node1.is_settle());
        // Check if both nodes have the same state
        assert_eq!(node1.this_state, node2.this_state);
        assert_eq!(node1.other_state, node2.other_state);
    }
}
