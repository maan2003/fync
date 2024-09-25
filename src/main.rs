use anyhow::Result;
use clap::{Parser, Subcommand};
use crossbeam_channel::{Receiver, RecvError, RecvTimeoutError, Sender};
use fync::{
    watch_root, ContentStore, Node, NodeInit, NodeInitMessage, NodeMessage, RefreshRequest,
};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::thread::scope;
use std::time::{Duration, Instant};
use tracing::{info, instrument};

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

fn sync_command(src: PathBuf, dst: PathBuf) -> Result<()> {
    let src_root = src.canonicalize()?;
    let dst_root = dst.canonicalize()?;

    let mut content_store_src = ContentStore::default();
    let mut content_store_dst = ContentStore::default();

    let (src_out, dst_in) = crossbeam_channel::bounded::<NodeInitMessage>(32);
    let (dst_out, src_in) = crossbeam_channel::bounded::<NodeInitMessage>(32);

    let src_init = NodeInit::from_disk(&src_root, &mut content_store_src, true)?;
    let dst_init = NodeInit::from_disk(&dst_root, &mut content_store_dst, false)?;

    let (src_node, dst_node) = scope(|s| {
        let source_handle =
            s.spawn(|| run_node_init(src_init, &src_root, src_in, src_out, &mut content_store_src));
        let dest_handle =
            s.spawn(|| run_node_init(dst_init, &dst_root, dst_in, dst_out, &mut content_store_dst));
        anyhow::Ok((source_handle.join().unwrap()?, dest_handle.join().unwrap()?))
    })?;

    info!("Initial sync completed successfully");

    let (dst_out, src_in) = crossbeam_channel::bounded::<NodeMessage>(32);
    let (src_out, dst_in) = crossbeam_channel::bounded::<NodeMessage>(32);
    scope(|s| {
        s.spawn(|| run_node(src_node, &src_root, src_in, src_out, &mut content_store_src));
        s.spawn(|| run_node(dst_node, &dst_root, dst_in, dst_out, &mut content_store_dst));
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
    root: &Path,
    input: Receiver<NodeInitMessage>,
    output: Sender<NodeInitMessage>,
    content_store: &mut ContentStore,
) -> Result<Node> {
    output.send(node_init.announce())?;
    loop {
        let init_message = input.recv()?;
        let (node, response) = node_init.handle_init_message(root, init_message, content_store)?;
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
    root: &Path,
    input: Receiver<NodeMessage>,
    output: Sender<NodeMessage>,
    content_store: &mut ContentStore,
) -> std::result::Result<(), anyhow::Error> {
    let (watch_tx, watch_rx) = crossbeam_channel::bounded(32);
    let _watcher = watch_root(root, move |paths| {
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
            Event::Message(msg) => node.handle_message_disk(msg, root, content_store)?,
            Event::Refresh(path_list) => node.refresh_requests(root, &path_list, content_store)?,
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
