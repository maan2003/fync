use anyhow::Result;
use clap::{Parser, Subcommand};
use fync::{watch_root, ContentStore, FsState, Node};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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

fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Watch { path } => watch_command(path),
        Commands::Sync {
            source,
            destination,
        } => sync_command(source, destination),
    }
}

fn watch_command(path: PathBuf) -> Result<()> {
    let mut content_store = ContentStore::default();
    let root = path.canonicalize().unwrap();
    let initial_state = FsState::from_disk(&root, &mut content_store)?;
    let node = Arc::new(Mutex::new(Node::new(initial_state.clone(), initial_state)));
    let content_store = Arc::new(Mutex::new(content_store));

    eprintln!("node = {:#?}", node);
    let node_clone = Arc::clone(&node);
    let root_clone = root.clone();
    let content_store_clone = Arc::clone(&content_store);
    let _watcher = watch_root(&root, move |paths| {
        let mut node = node_clone.lock().unwrap();
        let mut content_store = content_store_clone.lock().unwrap();
        match node.refresh_paths(&root_clone, &paths, &mut content_store) {
            Ok(diff) => {
                if !diff.is_empty() {
                    println!("Changes detected:");
                    println!("{:#?}", diff);
                }
            }
            Err(e) => eprintln!("Error refreshing paths: {}", e),
        }
    })?;

    println!("Press Ctrl+C to exit");

    // Keep the main thread running
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

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
    println!("Initial sync from {:?} to {:?}:", source, destination);
    println!("{:#?}", initial_source_diff);

    {
        let mut dest_node = dest_node.lock().unwrap();
        dest_node.apply_changes_from_other(&initial_source_diff)?;
        // TODO: sync stores
        initial_source_diff.apply_to_disk(&destination, &content_store.lock().unwrap())?;
        source_node
            .lock()
            .unwrap()
            .changes_acked_by_other(&initial_source_diff);
    }

    println!("Initial sync completed successfully");

    // Set up watchers for both directories
    let source_clone = source.clone();
    let dest_clone = destination.clone();
    let source_node_clone = Arc::clone(&source_node);
    let dest_node_clone = Arc::clone(&dest_node);
    let content_store_clone = Arc::clone(&content_store);

    let _source_watcher = watch_root(&source, move |paths| {
        let mut source_node = source_node_clone.lock().unwrap();
        let content_store = &mut content_store_clone.lock().unwrap();
        match source_node.refresh_paths(&source_clone, &paths, content_store) {
            Ok(diff) => {
                if !diff.is_empty() {
                    println!("Changes detected in source:");
                    println!("{:#?}", diff);
                    let mut dest_node = dest_node_clone.lock().unwrap();
                    eprintln!("no dead lock - dest?");
                    if let Err(e) = dest_node.apply_changes_from_other(&diff) {
                        eprintln!("Error applying changes to destination: {}", e);
                    }
                    if let Err(e) = diff.apply_to_disk(&dest_clone, &content_store) {
                        eprintln!("Error applying changes to destination disk: {}", e);
                    }
                }
            }
            Err(e) => eprintln!("Error refreshing source paths: {}", e),
        }
    })?;

    let source_clone = source.clone();
    let dest_clone = destination.clone();
    let source_node_clone = Arc::clone(&source_node);
    let dest_node_clone = Arc::clone(&dest_node);
    let content_store_clone = Arc::clone(&content_store);

    let _dest_watcher = watch_root(&destination, move |paths| {
        let mut dest_node = dest_node_clone.lock().unwrap();
        let content_store = &mut content_store_clone.lock().unwrap();
        match dest_node.refresh_paths(&dest_clone, &paths, content_store) {
            Ok(diff) => {
                if !diff.is_empty() {
                    println!("Changes detected in destination:");
                    println!("{:#?}", diff);
                    let mut source_node = source_node_clone.lock().unwrap();
                    if let Err(e) = source_node.apply_changes_from_other(&diff) {
                        eprintln!("Error applying changes to source: {}", e);
                    }
                    if let Err(e) = diff.apply_to_disk(&source_clone, content_store) {
                        eprintln!("Error applying changes to source disk: {}", e);
                    }
                    eprintln!("Applied Changes to Src");
                }
            }
            Err(e) => eprintln!("Error refreshing destination paths: {}", e),
        }
    })?;

    println!("Watching for changes. Press Ctrl+C to exit.");

    // Keep the main thread running
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
