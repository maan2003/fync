use anyhow::Result;
use std::{collections::BTreeMap, time::SystemTime};

fn main() {
    println!("Hello, world!");
}

struct FileName(Arc<str>);
struct FileMetadata {
    content_hash: iroh_blake3::Hash,
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
    pub fn apply_diff(&self, root: &Path) -> Result<()> {
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
                    std::fs::write(&full_path, &[])?;
                }
                FileChange::Modified { new_meta, .. } => {
                    // TODO: modify
                }
            }
        }
        Ok(())
    }
}
