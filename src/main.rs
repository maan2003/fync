use anyhow::Result;
use clap::{Parser, Subcommand};
use crossbeam_channel::{Receiver, RecvError, RecvTimeoutError, Sender};
use fync::{
    watch_root, ContentStore, FsState, Node, NodeInit, NodeInitMessage, NodeMessage, RefreshRequest,
};
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
    let _watcher = watch_root(&directory, move |paths| {
        let _ = watch_tx.send(paths);
    });

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
    let content_store = Arc::new(Mutex::new(content_store));

    let (source_init_out, dest_init_in) = crossbeam_channel::bounded::<NodeInitMessage>(32);
    let (dest_init_out, source_init_in) = crossbeam_channel::bounded::<NodeInitMessage>(32);

    let source_init = NodeInit::from_disk(&source, &mut content_store.lock().unwrap(), true)?;
    let dest_init = NodeInit::from_disk(&destination, &mut content_store.lock().unwrap(), false)?;

    let (source_node, dest_node) = scope(|s| {
        let source_handle = s.spawn(|| {
            run_node_init(
                source_init,
                source.clone(),
                source_init_in,
                source_init_out,
                content_store.clone(),
            )
        });
        let dest_handle = s.spawn(|| {
            run_node_init(
                dest_init,
                destination.clone(),
                dest_init_in,
                dest_init_out,
                content_store.clone(),
            )
        });
        anyhow::Ok((source_handle.join().unwrap()?, dest_handle.join().unwrap()?))
    })?;

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
fn run_node_init(
    mut node_init: NodeInit,
    root: PathBuf,
    input: Receiver<NodeInitMessage>,
    output: Sender<NodeInitMessage>,
    content_store: Arc<Mutex<ContentStore>>,
) -> Result<Node> {
    output.send(node_init.announce());
    loop {
        let init_message = input.recv()?;
        let (node, response) = node_init.handle_init_message(
            &root,
            init_message,
            &mut content_store.lock().unwrap(),
        )?;
        if let Some(response) = response {
            output.send(response)?;
        }
        if let Some(node) = node {
            return Ok(node);
        }
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
            Refresh(Vec<RefreshRequest>),
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
                node.refresh_requests(&root, &path_list, &mut content_store.lock().unwrap())?
            }
        };
        if let Some(response) = response {
            output.send(response)?;
        }
    }
    anyhow::Ok(())
}

fn debounce_watcher(
    path_list: Vec<RefreshRequest>,
    rx: &Receiver<Vec<RefreshRequest>>,
) -> Result<Vec<RefreshRequest>, RecvError> {
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
