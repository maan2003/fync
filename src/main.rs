use anyhow::Result;
use clap::{Parser, Subcommand};
use crossbeam_channel::{Receiver, RecvError, RecvTimeoutError, Sender};
use fync::{watch_root, watch_root_fanotify, ContentStore, FsState, Node, NodeMessage};
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
    Watch {
        directory: PathBuf,
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
        Commands::Watch { directory } => watch_command(directory),
    }
}

fn watch_command(directory: PathBuf) -> Result<()> {
    let (watch_tx, watch_rx) = crossbeam_channel::bounded(32);
    let _watcher = watch_root_fanotify(&directory, move |paths| {
        let _ = watch_tx.send(paths);
    });
    dbg!(_watcher);

    loop {
        match debounce_watcher(watch_rx.recv()?, &watch_rx) {
            Ok(paths) => {
                println!("Changes detected:");
                for path in paths {
                    println!("  {:?}", path);
                }
            }
            Err(RecvError) => break,
        }
    }

    Ok(())
}

fn sync_command(source: PathBuf, destination: PathBuf) -> Result<()> {
    let source = source.canonicalize()?;
    let destination = destination.canonicalize()?;

    let mut content_store = ContentStore::default();
    let source_state = FsState::from_disk(&source, &mut content_store)?;
    let dest_state = FsState::from_disk(&destination, &mut content_store)?;

    let content_store = Arc::new(Mutex::new(content_store));

    let mut source_node = Node::new(source_state.clone(), dest_state.clone());
    let mut dest_node = Node::new(dest_state, source_state);

    // Initial sync
    let initial_source_diff = source_node.changes_for_other();
    info!("Initial sync started");
    debug!(?initial_source_diff);

    dest_node.apply_changes_from_other_to_disk(
        &initial_source_diff,
        &destination,
        &mut content_store.lock().unwrap(),
    )?;
    source_node.changes_acked_by_other(&initial_source_diff);

    info!("Initial sync completed successfully");

    let (dest_out, source_in) = crossbeam_channel::bounded::<NodeMessage>(32);
    let (source_out, dest_in) = crossbeam_channel::bounded::<NodeMessage>(32);
    scope(|s| {
        s.spawn(|| {
            run_node(
                source_node,
                source,
                source_in,
                source_out,
                content_store.clone(),
            )
        });
        s.spawn(|| {
            run_node(
                dest_node,
                destination,
                dest_in,
                dest_out,
                content_store.clone(),
            )
        });
        info!("Watching for changes. Press Ctrl+C to exit.");
    });

    // Keep the main thread running
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

#[instrument(skip_all, fields(?root), err, ret)]
fn run_node(
    mut node: Node,
    root: PathBuf,
    input: Receiver<NodeMessage>,
    output: Sender<NodeMessage>,
    content_store: Arc<Mutex<ContentStore>>,
) -> std::result::Result<(), anyhow::Error> {
    let (watch_tx, watch_rx) = crossbeam_channel::bounded(32);
    let _watcher = watch_root(&root, move |paths| {
        // FIXME: stop watching if other side dies.
        let _ = watch_tx.send(paths);
    });
    loop {
        #[derive(Debug)]
        enum Event {
            Message(NodeMessage),
            Refresh(Vec<PathBuf>),
        }
        let event = crossbeam_channel::select! {
            recv(input) -> msg => {
                if let Ok(msg) = msg {
                    Event::Message(msg)
                } else {
                    break;
                }
            }
            recv(watch_rx) -> path_list => {
                match debounce_watcher(path_list?, &watch_rx) {
                    Ok(paths) => Event::Refresh(paths),
                    Err(RecvError) => break,
                }
            }
        };
        info!(?event, "processing event");
        let response = match event {
            Event::Message(msg) => {
                node.handle_message_disk(msg, &root, &mut content_store.lock().unwrap())?
            }
            Event::Refresh(path_list) => {
                node.refresh_paths(&root, &path_list, &mut content_store.lock().unwrap())?
            }
        };
        if let Some(response) = response {
            output.send(response)?;
        }
    }
    anyhow::Ok(())
}

fn debounce_watcher(
    path_list: Vec<PathBuf>,
    rx: &Receiver<Vec<PathBuf>>,
) -> Result<Vec<PathBuf>, RecvError> {
    let mut paths = BTreeSet::new();
    paths.extend(path_list);
    let debounce_deadline = Instant::now() + Duration::from_millis(20);
    loop {
        match rx.recv_deadline(debounce_deadline) {
            Ok(path_list) => {
                info!(?path_list, "more events");
                paths.extend(path_list)
            }
            Err(RecvTimeoutError::Timeout) => break,
            Err(RecvTimeoutError::Disconnected) => {
                if paths.is_empty() {
                    return Err(RecvError);
                }
                break;
            }
        }
    }
    info!(?paths, "refreshing");
    Ok(paths.into_iter().collect())
}
