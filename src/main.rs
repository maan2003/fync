use anyhow::Result;
use clap::Parser;
use fync::{watch_root, ContentStore, FsState, Node};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(default_value = ".")]
    path: PathBuf,
}

fn main() -> Result<()> {
    let args: Args = Args::parse();
    let root = args.path.canonicalize().unwrap();
    let initial_state = FsState::from_disk(&root)?;
    let node = Arc::new(Mutex::new(Node::new(
        initial_state.clone(),
        initial_state,
        ContentStore::default(),
    )));

    eprintln!("node = {:#?}", node);
    let node_clone = Arc::clone(&node);
    let _watcher = watch_root(&root.clone(), move |paths| {
        let mut node = node_clone.lock().unwrap();
        match node.refresh_paths(&root, &paths) {
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
