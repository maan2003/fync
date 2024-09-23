use anyhow::Result;
use clap::{Parser, Subcommand};
use crossbeam_channel::{RecvError, RecvTimeoutError};
use fync::{watch_root, ContentStore, FsState, Node};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::scope;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Watch {
        #[arg(default_value = ".")]
        path: PathBuf,
    },
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
        Commands::Watch { path } => watch_command(path),
        Commands::Sync {
            source,
            destination,
        } => sync_command(source, destination),
    }
}

#[instrument]
fn watch_command(path: PathBuf) -> Result<()> {
    let mut content_store = ContentStore::default();
    let root = path.canonicalize().unwrap();
    let initial_state = FsState::from_disk(&root, &mut content_store)?;
    let node = Arc::new(Mutex::new(Node::new(initial_state.clone(), initial_state)));
    let content_store = Arc::new(Mutex::new(content_store));

    debug!("Initial node state: {:#?}", node);
    let node_clone = Arc::clone(&node);
    let root_clone = root.clone();
    let content_store_clone = Arc::clone(&content_store);
    let _watcher = watch_root(&root, move |paths| {
        let mut node = node_clone.lock().unwrap();
        let mut content_store = content_store_clone.lock().unwrap();
        match node.refresh_paths(&root_clone, &paths, &mut content_store) {
            Ok(diff) => {
                if !diff.is_empty() {
                    info!("Changes detected: {:#?}", diff);
                }
            }
            Err(e) => error!("Error refreshing paths: {}", e),
        }
    })?;

    info!("Press Ctrl+C to exit");

    // Keep the main thread running
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
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
        let unconflicted_diff = dest_node.apply_changes_from_other(&initial_source_diff)?;
        // TODO: sync stores
        unconflicted_diff.apply_to_disk(&destination, &content_store.lock().unwrap())?;
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
            let mut is_disconnected = false;
            while !is_disconnected {
                let mut paths = BTreeSet::new();
                match rx.recv() {
                    Ok(path_list) => paths.extend(path_list),
                    Err(RecvError) => break,
                }
                let debounce_deadline = std::time::Instant::now() + Duration::from_millis(10);
                loop {
                    match rx.recv_deadline(debounce_deadline) {
                        Ok(path_list) => paths.extend(path_list),
                        Err(RecvTimeoutError::Timeout) => break,
                        Err(RecvTimeoutError::Disconnected) => {
                            is_disconnected = true;
                            break;
                        }
                    }
                }
                let paths = paths.into_iter().collect::<Vec<_>>();
                let mut source_node = source_node.lock().unwrap();
                let mut dest_node = dest_node.lock().unwrap();
                let content_store = &mut content_store.lock().unwrap();
                info!(?paths, "Changes detected in source");
                match source_node.refresh_paths(&source, &paths, content_store) {
                    Ok(diff) => {
                        if !diff.is_empty() {
                            debug!(?diff);
                            match dest_node.apply_changes_from_other(&diff) {
                                Ok(unconflicted_diff) => {
                                    if let Err(e) = unconflicted_diff
                                        .apply_to_disk(&destination, &content_store)
                                    {
                                        error!("Error applying changes to destination disk: {}", e);
                                    }
                                    source_node.changes_acked_by_other(&unconflicted_diff);
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
    });
    info!("Watching for changes. Press Ctrl+C to exit.");

    // Keep the main thread running
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
