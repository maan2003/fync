// FIXME: protect against attacks
// TODO: landlock support
// TODO: think about atomically writting files

use anyhow::{bail, Context, Result};
use iroh_blake3::Hash as ContentHash;
use notify_debouncer_full::notify::{
    self,
    event::{CreateKind, RemoveKind},
    RecommendedWatcher, RecursiveMode, Watcher,
};
use std::{
    collections::{btree_map, BTreeMap, HashMap, HashSet},
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::error;

/// A relative path to some root.
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

impl FilePath {
    fn from_root_and_path(path: &Path, root: &Path) -> Result<FilePath> {
        Ok(FilePath(Arc::from(
            path.strip_prefix(root)?.to_string_lossy().as_ref(),
        )))
    }

    fn to_absolute(&self, root: &Path) -> PathBuf {
        root.join(self.0.as_ref())
    }
}

impl FileMetadata {
    fn from_fs(file: &Path, content_store: &mut ContentStore) -> Result<Option<Self>> {
        match std::fs::read(file) {
            Ok(content) => {
                let content_hash = content_store.add(content);
                Ok(Some(FileMetadata { content_hash }))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

impl FsState {
    pub fn from_disk(root: &Path, content_store: &mut ContentStore) -> Result<Self> {
        let mut files = BTreeMap::new();
        for entry in ignore::Walk::new(root) {
            let entry = entry?;
            if entry.file_type().map_or(false, |d| d.is_file()) {
                let file_path = FilePath::from_root_and_path(entry.path(), root)?;
                if let Some(meta) = FileMetadata::from_fs(entry.path(), content_store)? {
                    files.insert(file_path, meta);
                }
            }
        }
        Ok(FsState { files })
    }

    pub fn refresh_path(
        &mut self,
        root: &Path,
        path: &Path,
        content_store: &mut ContentStore,
    ) -> Result<Option<(FilePath, FileChange)>> {
        let file_path = FilePath::from_root_and_path(path, root)?;

        let new_metadata = FileMetadata::from_fs(path, content_store);
        if let Ok(Some(new_metadata)) = new_metadata {
            match self.files.entry(file_path.clone()) {
                btree_map::Entry::Occupied(mut entry) => {
                    if entry.get().content_hash != new_metadata.content_hash {
                        let old_metadata = entry.insert(new_metadata.clone());
                        Ok(Some((
                            file_path,
                            FileChange::Modified {
                                old_meta: old_metadata,
                                new_meta: new_metadata,
                            },
                        )))
                    } else {
                        Ok(None)
                    }
                }
                btree_map::Entry::Vacant(entry) => {
                    entry.insert(new_metadata.clone());
                    Ok(Some((
                        file_path,
                        FileChange::Created { meta: new_metadata },
                    )))
                }
            }
        } else if !path.exists() {
            if let Some(old_metadata) = self.files.remove(&file_path) {
                Ok(Some((
                    file_path,
                    FileChange::Removed {
                        old_meta: old_metadata,
                    },
                )))
            } else {
                Ok(None)
            }
        } else {
            // noop for other file types
            Ok(None)
        }
    }
    pub fn refresh_full_rescan(
        &mut self,
        root: &Path,
        directory: &Path,
        content_store: &mut ContentStore,
    ) -> Result<FsStateDiff> {
        let mut diff = FsStateDiff {
            files: BTreeMap::new(),
        };
        let full_dir_path = root.join(directory);
        let dir_prefix = FilePath::from_root_and_path(&full_dir_path, root)?;

        // Remove entries that no longer exist
        let removed: Vec<_> = self
            .files
            .range(dir_prefix.clone()..)
            .take_while(|(k, _)| k.0.starts_with(&*dir_prefix.0))
            .filter(|(k, _)| !k.to_absolute(root).exists())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (file_path, metadata) in removed {
            self.files.remove(&file_path);
            diff.files
                .insert(file_path, FileChange::Removed { old_meta: metadata });
        }

        // Walk the directory and update/add entries
        for entry in ignore::Walk::new(&full_dir_path) {
            let Ok(entry) = entry else {
                continue;
            };
            if entry.file_type().map_or(false, |d| d.is_file()) {
                if let Some((file_path, change)) =
                    self.refresh_path(root, entry.path(), content_store)?
                {
                    diff.files.insert(file_path, change);
                }
            }
        }
        Ok(diff)
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

    // Returns the list conflicted paths
    pub fn apply_diff(&mut self, diff: &FsStateDiff) -> Vec<FilePath> {
        let mut conflicts = Vec::new();
        for (file_path, change) in &diff.files {
            let current_status = self.files.get(file_path);
            if change.conflicts(current_status) {
                conflicts.push(file_path.clone());
            } else {
                match change {
                    FileChange::Removed { .. } => {
                        self.files.remove(file_path);
                    }
                    FileChange::Created { meta } | FileChange::Modified { new_meta: meta, .. } => {
                        self.files.insert(file_path.clone(), meta.clone());
                    }
                }
            }
        }

        conflicts
    }

    pub fn apply_diff_to_disk(
        &mut self,
        diff: &FsStateDiff,
        root: &Path,
        content_store: &mut ContentStore,
    ) -> Result<Vec<FilePath>> {
        let mut conflicts = Vec::new();
        for (file_path, change) in &diff.files {
            let full_path = file_path.to_absolute(root);
            let metadata = FileMetadata::from_fs(&full_path, content_store)?;
            if change.conflicts(metadata.as_ref()) {
                conflicts.push(file_path.clone());
                continue;
            }
            match change {
                FileChange::Removed { .. } => {
                    if let Err(e) = std::fs::remove_file(&full_path) {
                        if e.kind() != ErrorKind::NotFound {
                            return Err(e.into());
                        }
                    }
                    self.files.remove(file_path);
                }
                FileChange::Created { meta } | FileChange::Modified { new_meta: meta, .. } => {
                    if let Some(parent) = full_path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    let content = content_store.get(&meta.content_hash)?;
                    std::fs::write(&full_path, content)?;
                    self.files.insert(file_path.clone(), meta.clone());
                }
            }
        }
        Ok(conflicts)
    }
}

#[derive(Debug)]
pub struct FsStateDiff {
    // TODO: avoid sending same file path again and again
    files: BTreeMap<FilePath, FileChange>,
}

#[derive(Debug, Clone)]
pub enum FileChange {
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

impl FileChange {
    fn conflicts(&self, current_metadata: Option<&FileMetadata>) -> bool {
        match (current_metadata, self) {
            (Some(current_meta), FileChange::Removed { old_meta }) => {
                current_meta.content_hash != old_meta.content_hash
            }
            (None, FileChange::Removed { .. }) => false,

            (None, FileChange::Created { .. }) => false,
            (Some(current_meta), FileChange::Created { meta }) => {
                current_meta.content_hash != meta.content_hash
            }

            (Some(current_meta), FileChange::Modified { old_meta, new_meta }) => {
                // Check if the current state differs from both old and new states
                current_meta.content_hash != old_meta.content_hash
                    && current_meta.content_hash != new_meta.content_hash
            }
            (None, FileChange::Modified { .. }) => true,
        }
    }
}

impl FsStateDiff {
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

#[derive(Default, Debug)]
pub struct ContentStore {
    // todo: use better hash map
    contents: HashMap<ContentHash, Vec<u8>>,
    new_contents: Vec<ContentHash>,
}

impl ContentStore {
    pub fn add(&mut self, content: Vec<u8>) -> ContentHash {
        let hash = iroh_blake3::hash(&content);
        self.insert(hash, content);
        hash
    }

    pub fn insert(&mut self, hash: ContentHash, content: Vec<u8>) {
        if self.contents.insert(hash, content).is_none() {
            self.new_contents.push(hash);
        }
    }

    pub fn get(&self, hash: &ContentHash) -> Result<&[u8]> {
        self.contents
            .get(hash)
            .context("Content not found in content store")
            .map(|x| x.as_slice())
    }

    pub fn remove(&mut self, hash: &ContentHash) {
        self.contents.remove(hash);
    }

    pub fn has(&self, hash: &ContentHash) -> bool {
        self.contents.contains_key(hash)
    }

    pub fn drain_new_contents(&mut self) -> Vec<ContentHash> {
        std::mem::take(&mut self.new_contents)
    }
}

#[derive(Debug)]
pub struct Node {
    this_state: FsState,
    other_state: FsState,
    conflicts: Vec<FilePath>,
}

pub enum NodeMessage {
    Changes {
        new_content: Vec<Vec<u8>>,
        diff: FsStateDiff,
    },
    ChangesResponse {
        accepted_diff: FsStateDiff,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RefreshRequest {
    FullRescan(PathBuf),
    Path(PathBuf),
}

impl std::fmt::Debug for NodeMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeMessage::Changes { new_content, diff } => f
                .debug_struct("Changes")
                .field("new_content", &new_content.len())
                .field("diff", diff)
                .finish(),
            NodeMessage::ChangesResponse { accepted_diff } => f
                .debug_struct("ChangesResponse")
                .field("accepted_diff", accepted_diff)
                .finish(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeInit {
    this_state: FsState,
    other_state: Option<FsState>,
    should_override: bool,
}

#[derive(Debug, Clone)]
pub enum NodeInitMessage {
    NodeAnnouncement { state: FsState },
    Override { new_content: Vec<Vec<u8>> },
    OverrideAck,
}

pub enum AnyNodeMessage {
    Init(NodeInitMessage),
    Regular(NodeMessage),
}

impl NodeInit {
    pub fn from_disk(
        root: &Path,
        content_store: &mut ContentStore,
        should_override: bool,
    ) -> Result<Self> {
        let this_state = FsState::from_disk(root, content_store)?;
        Ok(Self {
            this_state,
            other_state: None,
            should_override,
        })
    }
    pub fn announce(&self) -> NodeInitMessage {
        NodeInitMessage::NodeAnnouncement {
            state: self.this_state.clone(),
        }
    }

    fn override_other(&mut self, content_store: &mut ContentStore) -> NodeInitMessage {
        let new_content_hashes = content_store.drain_new_contents();
        let other_hashes: HashSet<_> = self
            .other_state
            .as_ref()
            .unwrap()
            .files
            .values()
            .map(|meta| meta.content_hash)
            .collect();
        let new_content: Vec<Vec<u8>> = new_content_hashes
            .into_iter()
            .filter(|hash| !other_hashes.contains(hash))
            .map(|hash| content_store.get(&hash).unwrap().to_vec())
            .collect();
        NodeInitMessage::Override { new_content }
    }

    pub fn handle_init_message(
        &mut self,
        root: &Path,
        message: NodeInitMessage,
        content_store: &mut ContentStore,
    ) -> Result<(Option<Node>, Option<NodeInitMessage>)> {
        match message {
            NodeInitMessage::NodeAnnouncement { state: other_state } => {
                self.other_state = Some(other_state);
                if self.should_override {
                    Ok((None, Some(self.override_other(content_store))))
                } else {
                    Ok((None, None))
                }
            }
            NodeInitMessage::Override { new_content } => {
                if self.other_state.is_none() {
                    bail!("Cannot apply override without other state");
                }
                for content in new_content {
                    content_store.add(content);
                }
                let diff = self.this_state.diff(self.other_state.as_ref().unwrap());
                self.other_state.as_mut().unwrap().apply_diff_to_disk(
                    &diff,
                    root,
                    content_store,
                )?;
                let node = Node::new(self.this_state.clone(), self.other_state.take().unwrap());
                Ok((Some(node), Some(NodeInitMessage::OverrideAck)))
            }
            NodeInitMessage::OverrideAck => {
                if self.other_state.is_none() {
                    bail!("Cannot accept override ask without other state");
                }
                let node = Node::new(self.this_state.clone(), self.other_state.take().unwrap());
                Ok((Some(node), None))
            }
        }
    }
}

impl Node {
    pub fn new(this_state: FsState, other_state: FsState) -> Self {
        Self {
            this_state,
            other_state,
            conflicts: Vec::new(),
        }
    }

    pub fn messages_for_other(&mut self, content_store: &mut ContentStore) -> NodeMessage {
        let diff = self.changes_for_other();
        let new_content_hashes = content_store.drain_new_contents();
        let new_content = new_content_hashes
            .into_iter()
            .map(|hash| content_store.get(&hash).unwrap().to_vec())
            .collect();

        NodeMessage::Changes { new_content, diff }
    }

    pub fn handle_message_disk(
        &mut self,
        message: NodeMessage,
        root: &Path,
        content_store: &mut ContentStore,
    ) -> Result<Option<NodeMessage>> {
        match message {
            NodeMessage::Changes { new_content, diff } => {
                for content in new_content {
                    content_store.add(content);
                }
                let accepted_diff =
                    self.apply_changes_from_other_to_disk(&diff, root, content_store)?;
                Ok(Some(NodeMessage::ChangesResponse { accepted_diff }))
            }
            NodeMessage::ChangesResponse { accepted_diff } => {
                self.changes_acked_by_other(&accepted_diff);
                Ok(None)
            }
        }
    }

    pub fn handle_message_mem(
        &mut self,
        message: NodeMessage,
        content_store: &mut ContentStore,
    ) -> Result<Option<NodeMessage>> {
        match message {
            NodeMessage::Changes { new_content, diff } => {
                for content in new_content {
                    content_store.add(content);
                }
                let accepted_diff = self.apply_changes_from_other_mem(&diff);
                Ok(Some(NodeMessage::ChangesResponse { accepted_diff }))
            }
            NodeMessage::ChangesResponse { accepted_diff } => {
                self.changes_acked_by_other(&accepted_diff);
                Ok(None)
            }
        }
    }

    pub fn changes_for_other(&mut self) -> FsStateDiff {
        self.other_state.diff(&self.this_state)
    }

    pub fn changes_acked_by_other(&mut self, diff: &FsStateDiff) {
        let conflicts = self.other_state.apply_diff(diff);
        if !conflicts.is_empty() {
            error!("Unexpected conflicts in acked changes");
        }
    }

    pub fn apply_changes_from_other_mem(&mut self, diff: &FsStateDiff) -> FsStateDiff {
        let conflicts = self.other_state.apply_diff(diff);
        if !conflicts.is_empty() {
            error!("Unexpected conflicts in other state");
        }
        let conflicts = self.this_state.apply_diff(&diff);
        let accepted_diff = FsStateDiff {
            files: diff
                .files
                .iter()
                .filter(|(file_path, _change)| !conflicts.contains(file_path))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };
        self.conflicts.extend(conflicts);
        accepted_diff
    }

    pub fn apply_changes_from_other_to_disk(
        &mut self,
        diff: &FsStateDiff,
        root: &Path,
        content_store: &mut ContentStore,
    ) -> anyhow::Result<FsStateDiff> {
        let conflicts = self
            .this_state
            .apply_diff_to_disk(diff, root, content_store)?;
        let accepted_diff = FsStateDiff {
            files: diff
                .files
                .iter()
                .filter(|(file_path, _change)| !conflicts.contains(file_path))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };
        self.conflicts.extend(conflicts);
        Ok(accepted_diff)
    }

    pub fn refresh_requests(
        &mut self,
        root: &Path,
        requests: &[RefreshRequest],
        content_store: &mut ContentStore,
    ) -> Result<Option<NodeMessage>> {
        let mut diff = FsStateDiff {
            files: BTreeMap::new(),
        };
        for request in requests {
            match request {
                RefreshRequest::FullRescan(path) => {
                    let dir_diff =
                        self.this_state
                            .refresh_full_rescan(root, path, content_store)?;
                    diff.files.extend(dir_diff.files);
                }
                RefreshRequest::Path(path) => {
                    if let Some((file_path, change)) =
                        self.this_state.refresh_path(root, path, content_store)?
                    {
                        diff.files.insert(file_path, change);
                    }
                }
            }
        }
        if diff.is_empty() {
            return Ok(None);
        }
        let new_content_hashes = content_store.drain_new_contents();
        let new_content = new_content_hashes
            .into_iter()
            .map(|hash| content_store.get(&hash).unwrap().to_vec())
            .collect();

        Ok(Some(NodeMessage::Changes { new_content, diff }))
    }

    pub fn has_conflicts(&self) -> bool {
        !self.conflicts.is_empty()
    }

    pub fn is_settle(&self) -> bool {
        self.this_state == self.other_state
    }
}

// TODO: don't watch git ignored paths
// so we get git ignored files in diffs :/
pub fn watch_root(
    root: &Path,
    handler: impl Fn(Vec<RefreshRequest>) + Send + 'static,
) -> Result<RecommendedWatcher> {
    let mut watcher = notify::RecommendedWatcher::new(
        move |result: Result<notify::Event, _>| {
            let Ok(event) = result else {
                error!("Error in file watcher");
                return;
            };
            let is_dir = match event.kind {
                notify::EventKind::Any => false,
                notify::EventKind::Access(_) => return,
                notify::EventKind::Create(x) => x == CreateKind::Folder,
                notify::EventKind::Modify(_) => false,
                notify::EventKind::Remove(x) => x == RemoveKind::Folder,
                notify::EventKind::Other => false,
            };
            let requests: Vec<RefreshRequest> = event
                .paths
                .into_iter()
                .map(if is_dir {
                    RefreshRequest::FullRescan
                } else {
                    RefreshRequest::Path
                })
                .collect();
            handler(requests);
        },
        Default::default(),
    )?;

    // TODO: check how this interacts with new directories
    // FIXME: this wasted effort by walking the tree *once again*
    watcher.watch(root, RecursiveMode::Recursive)?;
    Ok(watcher)
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
        let conflicts = state1.apply_diff(&diff);
        assert!(conflicts.is_empty());

        assert_eq!(state1, state2);
    }

    #[test]
    fn test_node() {
        let mut cs = ContentStore::default();

        let h1 = cs.add(b"hello world".to_vec());
        let h2 = cs.add(b"bye world".to_vec());

        let mut state1 = FsState::empty();
        state1.insert_file("file1.txt", h1);
        state1.insert_file("file2.txt", h2);

        let mut state2 = FsState::empty();
        state2.insert_file("file1.txt", h1);
        state2.insert_file("file3.txt", h2);

        let mut node1 = Node::new(state1.clone(), state2.clone());
        let mut node2 = Node::new(state2.clone(), state1.clone());

        assert!(!node1.is_settle());
        assert!(!node2.is_settle());

        // Simulate changes on node1
        let diff1 = node1.changes_for_other();
        node2.apply_changes_from_other_mem(&diff1);

        assert!(!node2.has_conflicts());
        assert!(node2.is_settle());

        assert!(!node1.is_settle());
        // Simulate changes on node2
        node1.changes_acked_by_other(&diff1);

        assert!(!node1.has_conflicts());
        assert_eq!(node1.this_state, node2.other_state);
        // Check if both nodes have the same state
        assert_eq!(node1.this_state, node2.this_state);
        assert_eq!(node1.other_state, node2.other_state);
    }

    #[test]
    fn test_concurrent_changes() {
        let mut cs = ContentStore::default();

        let h1 = cs.add(b"content1".to_vec());
        let h2 = cs.add(b"content2".to_vec());
        let h3 = cs.add(b"content3".to_vec());

        let mut state1 = FsState::empty();
        state1.insert_file("file1.txt", h1);
        state1.insert_file("file2.txt", h2);

        // file1 rm
        let mut state2 = state1.clone();
        state2.remove_file("file1.txt");

        let mut state3 = state1.clone();
        // file3 create
        state3.insert_file("file3.txt", h3);

        let mut node1 = Node::new(state2, state1.clone());
        let mut node2 = Node::new(state3, state1);
        let diff_for_1 = node2.changes_for_other();
        let diff_for_2 = node1.changes_for_other();

        // Apply changes to both nodes
        node1.apply_changes_from_other_mem(&diff_for_1);
        node2.apply_changes_from_other_mem(&diff_for_2);

        // Check for conflicts
        assert!(!node1.has_conflicts());
        assert!(!node2.has_conflicts());

        // Verify that the nodes are not settled
        assert_ne!(node1.this_state, node1.other_state);

        // Verify that the states are different
        assert_eq!(node1.this_state, node2.this_state);

        node1.changes_acked_by_other(&diff_for_2);
        node2.changes_acked_by_other(&diff_for_1);

        assert!(node1.is_settle());
        assert!(node2.is_settle());
    }

    #[test]
    fn test_conflicting_changes() {
        let mut cs = ContentStore::default();

        let h1 = cs.add(b"content1".to_vec());
        let h2 = cs.add(b"content2".to_vec());
        let h3 = cs.add(b"content3".to_vec());

        let mut state1 = FsState::empty();
        state1.insert_file("file1.txt", h1);
        state1.insert_file("file2.txt", h2);

        // Node 1 modifies file1.txt
        let mut state2 = state1.clone();
        state2.insert_file("file1.txt", h3);

        // Node 2 also modifies file1.txt, but differently
        let mut state3 = state1.clone();
        state3.insert_file("file1.txt", h2);

        let mut node1 = Node::new(state2, state1.clone());
        let mut node2 = Node::new(state3, state1);

        let diff_for_1 = node2.changes_for_other();
        let diff_for_2 = node1.changes_for_other();

        // Apply changes to both nodes
        node1.apply_changes_from_other_mem(&diff_for_1);
        node2.apply_changes_from_other_mem(&diff_for_2);

        // Check for conflicts
        assert!(node1.has_conflicts());
        assert!(node2.has_conflicts());

        // Verify that the nodes are not settled
        assert!(!node1.is_settle());
        assert!(!node2.is_settle());

        // Verify that the conflicted file is "file1.txt"
        assert_eq!(node1.conflicts, vec![FilePath(Arc::from("file1.txt"))]);
        assert_eq!(node2.conflicts, vec![FilePath(Arc::from("file1.txt"))]);
    }

    #[test]
    fn test_conflicting_concurrent_create() {
        let mut cs = ContentStore::default();

        let h1 = cs.add(b"content1".to_vec());
        let h2 = cs.add(b"content2".to_vec());

        let mut state1 = FsState::empty();
        state1.insert_file("file1.txt", h1);

        // Node 1 creates a new file
        let mut state2 = state1.clone();
        state2.insert_file("file2.txt", h2);

        // Node 2 modifies the same file
        let mut state3 = state1.clone();
        state3.insert_file("file2.txt", h1);

        let mut node1 = Node::new(state2, state1.clone());
        let mut node2 = Node::new(state3, state1);

        let diff_for_1 = node2.changes_for_other();
        let diff_for_2 = node1.changes_for_other();

        // Apply changes to both nodes
        node1.apply_changes_from_other_mem(&diff_for_1);
        node2.apply_changes_from_other_mem(&diff_for_2);

        // Check for conflicts
        assert!(node1.has_conflicts());
        assert!(node2.has_conflicts());

        // Verify that the conflicted file is "file2.txt"
        assert_eq!(node1.conflicts, vec![FilePath(Arc::from("file2.txt"))]);
        assert_eq!(node2.conflicts, vec![FilePath(Arc::from("file2.txt"))]);
    }

    #[test]
    fn test_concurrent_create_same_file_same_content() {
        let mut cs = ContentStore::default();

        let h1 = cs.add(b"content1".to_vec());
        let h2 = cs.add(b"same_content".to_vec());

        let mut state1 = FsState::empty();
        state1.insert_file("file1.txt", h1);

        // Node 1 creates a new file
        let mut state2 = state1.clone();
        state2.insert_file("file2.txt", h2);

        // Node 2 creates the same file with the same content
        let mut state3 = state1.clone();
        state3.insert_file("file2.txt", h2);

        let mut node1 = Node::new(state2.clone(), state1.clone());
        let mut node2 = Node::new(state3.clone(), state1.clone());

        let diff_for_1 = node2.changes_for_other();
        let diff_for_2 = node1.changes_for_other();

        // Apply changes to both nodes
        node1.apply_changes_from_other_mem(&diff_for_1);
        node2.apply_changes_from_other_mem(&diff_for_2);

        // Check for conflicts (should be none)
        assert!(!node1.has_conflicts());
        assert!(!node2.has_conflicts());

        // Verify that the nodes are settled
        assert!(node1.is_settle());
        assert!(node2.is_settle());

        // Verify that both nodes have the same state
        assert_eq!(node1.this_state, node2.this_state);

        assert_eq!(node1.this_state, state3);
        assert_eq!(node1.this_state, state2);
    }

    // Message tests
}
