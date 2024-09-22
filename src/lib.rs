use anyhow::Result;
use iroh_blake3::Hash as ContentHash;
use std::{collections::BTreeMap, time::SystemTime};

struct FileName(Arc<str>);
struct FileMetadata {
    content_hash: ContentHash,
    mtime: SystemTime,
}

struct FsState {
    files: BTreeMap<FileName, FileMetadata>,
}

impl FsState {
    pub fn from_root(root: PathBuf) -> Result<Self> {
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
                let file_name = FileName(Arc::from(path.to_string_lossy().as_ref()));
                let metadata = FileMetadata {
                    content_hash,
                    mtime,
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
    files: BTreeMap<FileName, FileChange>,
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
    pub fn apply_diff(&self, root: &Path, content_store: &ContentStore) -> Result<()> {
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
}

#[derive(Default, Debug)]
struct ContentStore {
    contents: BTreeMap<ContentHash, Vec<u8>>,
}

impl ContentStore {
    fn add(&mut self, content: Vec<u8>) -> ContentHash {
        let hash = iroh_blake3::hash(&content);
        self.contents.insert(hash, content);
        hash
    }

    fn get(&self, hash: &ContentHash) -> Result<&[u8]> {
        self.contents
            .get(hash)
            .ok_or_else(|| anyhow::anyhow!("Content not found in store"))
    }

    fn remove(&mut self, hash: &ContentHash) {
        self.contents.remove(hash);
    }

    fn has(&self, hash: &ContentHash) -> bool {
        self.contents.contains_key(hash)
    }
}
