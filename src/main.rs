use anyhow::Result;
use clap::{Parser, Subcommand};
use crossbeam_channel::{Receiver, RecvError, RecvTimeoutError};
use fync::{watch_root, ContentStore, FsState, Node};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::scope;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, warn};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Sync {
        source: PathBuf,
        destination: PathBuf,
    },
}

#[instrument]
fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    match args.command {
        Commands::Sync {
            source,
            destination,
        } => sync_command(source, destination),
    }
}

#[instrument]
fn sync_command(source: PathBuf, destination: PathBuf) -> Result<()> {
    let source = source.canonicalize()?;
    let destination = destination.canonicalize()?;

    let mut content_store = ContentStore::default();
    let source_state = FsState::from_disk(&source, &mut content_store)?;
    let dest_state = FsState::from_disk(&destination, &mut content_store)?;

    let content_store = Arc::new(Mutex::new(content_store));

    let source_node = Arc::new(Mutex::new(Node::new(
        source_state.clone(),
        dest_state.clone(),
    )));
    let dest_node = Arc::new(Mutex::new(Node::new(dest_state, source_state)));

    // Initial sync
    let initial_source_diff = {
        let mut source_node = source_node.lock().unwrap();
        source_node.changes_for_other()
    };
    info!("Initial sync started");
    debug!(?initial_source_diff);

    {
        let mut dest_node = dest_node.lock().unwrap();
        dest_node.apply_changes_from_other_to_disk(
            &initial_source_diff,
            &destination,
            &mut content_store.lock().unwrap(),
        )?;
        source_node
            .lock()
            .unwrap()
            .changes_acked_by_other(&initial_source_diff);
    }

    info!("Initial sync completed successfully");

    scope(|s| {
        s.spawn(|| {
            let (tx, rx) = crossbeam_channel::bounded(32);
            let _watcher = watch_root(&source, move |paths| {
                // FIXME: stop watching if other side dies.
                let _ = tx.send(paths);
            });
            loop {
                let paths = match debounce_watcher(&rx) {
                    Ok(paths) => paths,
                    Err(RecvError) => break,
                };
                let mut source_node = source_node.lock().unwrap();
                let mut dest_node = dest_node.lock().unwrap();
                let content_store = &mut content_store.lock().unwrap();
                info!(?paths, "Changes detected in source");
                match source_node.refresh_paths(&source, &paths, content_store) {
                    Ok(diff) => {
                        if !diff.is_empty() {
                            debug!(?diff);
                            match dest_node.apply_changes_from_other_to_disk(
                                &diff,
                                &source,
                                content_store,
                            ) {
                                Ok(()) => {
                                    // TODO: only unconflicted diff
                                    source_node.changes_acked_by_other(&diff);
                                }
                                Err(e) => {
                                    error!("Error applying changes to destination: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => error!("Error refreshing source paths: {}", e),
                }
            }
        });
        s.spawn(|| {
            let (tx, rx) = crossbeam_channel::bounded(32);
            let _watcher = watch_root(&destination, move |paths| {
                // FIXME: stop watching if other side dies.
                let _ = tx.send(paths);
            });
            loop {
                let paths = match debounce_watcher(&rx) {
                    Ok(paths) => paths,
                    Err(RecvError) => break,
                };
                let mut source_node = source_node.lock().unwrap();
                let mut dest_node = dest_node.lock().unwrap();
                let content_store = &mut content_store.lock().unwrap();
                info!(?paths, "Changes detected in destination");
                match dest_node.refresh_paths(&destination, &paths, content_store) {
                    Ok(diff) => {
                        if !diff.is_empty() {
                            debug!(?diff);
                            match source_node.apply_changes_from_other_to_disk(
                                &diff,
                                &source,
                                content_store,
                            ) {
                                Ok(()) => {
                                    dest_node.changes_acked_by_other(&diff);
                                }
                                Err(e) => {
                                    error!("Error applying changes to destination: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => error!("Error refreshing destination paths: {}", e),
                }
            }
        });
    });
    info!("Watching for changes. Press Ctrl+C to exit.");

    // Keep the main thread running
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn debounce_watcher(rx: &Receiver<Vec<PathBuf>>) -> Result<Vec<PathBuf>, RecvError> {
    let mut paths = BTreeSet::new();
    let path_list = rx.recv()?;
    paths.extend(path_list);
    let debounce_deadline = Instant::now() + Duration::from_millis(10);
    loop {
        match rx.recv_deadline(debounce_deadline) {
            Ok(path_list) => paths.extend(path_list),
            Err(RecvTimeoutError::Timeout) => break,
            Err(RecvTimeoutError::Disconnected) => {
                if paths.is_empty() {
                    return Err(RecvError);
                }
                break;
            }
        }
    }
    Ok(paths.into_iter().collect())
}
