use anyhow::{bail, Context, Result};
use bincode::config::standard;
use clap::{Parser, Subcommand};
use crossbeam_channel::{Receiver, RecvError, RecvTimeoutError, Sender};
use fync::{watch_root, AnyNodeMessage, ContentStore, NodeInit, NodeMessage, RefreshRequest};
use regex::Regex;
use std::collections::BTreeSet;
use std::io::{stderr, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread::scope;
use std::thread::sleep;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
    #[arg(short, default_value = "\\.git")]
    ignore_regex: String,
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
    RunStdio {
        root: PathBuf,
        #[arg(short)]
        override_other: bool,
    },
    SshSync {
        local_root: PathBuf,
        remote_host: String,
        remote_root: PathBuf,
        #[arg(short)]
        override_remote: bool,
    },
}

#[instrument]
fn main() -> Result<()> {
    tracing_subscriber::fmt::fmt().with_writer(stderr).init();
    let args = Args::parse();

    let regex = Regex::new(&args.ignore_regex)?;
    match args.command {
        Commands::Sync {
            source,
            destination,
        } => sync_command(source, destination, &regex),
        Commands::Watch { directory } => watch_command(directory, &regex),
        Commands::RunStdio {
            root,
            override_other,
        } => run_node_stdio(&root, override_other, &regex),
        Commands::SshSync {
            local_root,
            remote_host,
            remote_root,
            override_remote,
        } => ssh_sync_command_with_retry(
            local_root,
            remote_host,
            remote_root,
            override_remote,
            &args.ignore_regex,
        ),
    }
}

fn watch_command(directory: PathBuf, ignore: &Regex) -> Result<()> {
    let (watch_tx, watch_rx) = crossbeam_channel::bounded(32);
    let _watcher = watch_root(&directory, move |paths| {
        let _ = watch_tx.send(paths);
    });

    loop {
        match debounce_watcher(watch_rx.recv()?, &watch_rx, ignore) {
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

fn sync_command(src: PathBuf, dst: PathBuf, ignore: &Regex) -> Result<()> {
    let src_root = src.canonicalize()?;
    let dst_root = dst.canonicalize()?;

    let (dst_out, src_in) = crossbeam_channel::bounded::<AnyNodeMessage>(32);
    let (src_out, dst_in) = crossbeam_channel::bounded::<AnyNodeMessage>(32);
    scope(|s| {
        s.spawn(|| run_node(&src_root, src_in, src_out, true, ignore));
        s.spawn(|| run_node(&dst_root, dst_in, dst_out, false, ignore));
        info!("Watching for changes. Press Ctrl+C to exit.");
    });
    Ok(())
}

fn run_node_with_io<R: Read + Send + 'static, W: Write + Send + 'static>(
    root: &Path,
    override_other: bool,
    ignore: &Regex,
    reader: R,
    writer: W,
) -> Result<()> {
    let (input_tx, input_rx) = crossbeam_channel::unbounded();
    let (output_tx, output_rx) = crossbeam_channel::unbounded();

    let read_thread = std::thread::spawn(move || -> Result<()> {
        let mut reader = BufReader::new(reader);
        loop {
            let msg = bincode::decode_from_reader(&mut reader, standard())?;
            input_tx.send(msg)?;
        }
    });

    let write_thread = std::thread::spawn(move || {
        let mut writer = BufWriter::new(writer);
        while let Ok(msg) = output_rx.recv() {
            bincode::encode_into_std_write(&msg, &mut writer, standard())?;
            writer.flush()?;
        }
        anyhow::Ok(())
    });

    let result = run_node(&root, input_rx, output_tx, override_other, &ignore);

    read_thread
        .join()
        .expect("read thread panicked")
        .context("read thread")?;
    write_thread
        .join()
        .expect("write thread panicked")
        .context("write thread")?;

    result
}

fn run_node_stdio(root: &Path, override_other: bool, ignore: &Regex) -> Result<()> {
    let root = root.canonicalize()?;
    run_node_with_io(
        &root,
        override_other,
        ignore,
        std::io::stdin(),
        std::io::stdout(),
    )
}

#[instrument(skip_all, fields(?root), err, ret)]
fn run_node(
    root: &Path,
    input: Receiver<AnyNodeMessage>,
    output: Sender<AnyNodeMessage>,
    override_other: bool,
    ignore: &Regex,
) -> std::result::Result<(), anyhow::Error> {
    // first start watching
    let (watch_tx, watch_rx) = crossbeam_channel::bounded(32);
    let _watcher = watch_root(root, move |paths| {
        // FIXME: stop watching if other side dies.
        let _ = watch_tx.send(paths);
    });
    let content_store = &mut ContentStore::default();
    let mut node_init = NodeInit::from_disk(root, content_store, override_other)?;
    output.send(AnyNodeMessage::Init(node_init.announce()))?;
    let mut node = loop {
        let AnyNodeMessage::Init(init_message) = input.recv()? else {
            bail!("only expected init message");
        };
        let (node, response) = node_init.handle_init_message(root, init_message, content_store)?;
        if let Some(response) = response {
            output.send(AnyNodeMessage::Init(response))?;
        }
        if let Some(node) = node {
            break node;
        }
    };

    info!("Initial sync completed successfully");

    loop {
        #[derive(Debug)]
        enum Event {
            Message(NodeMessage),
            Refresh(Vec<RefreshRequest>),
        }
        let event = crossbeam_channel::select! {
            recv(input) -> msg => {
                if let Ok(msg) = msg {
                    let AnyNodeMessage::Regular(msg) = msg else {
                        bail!("only expected regular message, found init message");
                    };
                    Event::Message(msg)
                } else {
                    break;
                }
            }
            recv(watch_rx) -> path_list => {
                match debounce_watcher(path_list?, &watch_rx, ignore) {
                    Ok(paths) => Event::Refresh(paths),
                    Err(RecvError) => break,
                }
            }
        };
        debug!(?event, "Processing event");
        let response = match event {
            Event::Message(msg) => {
                match &msg {
                    NodeMessage::Changes { diff, .. } => {
                        info!("Received Changes: {} files", diff.files.len());
                    }
                    NodeMessage::ChangesResponse { accepted_diff } => {
                        info!(
                            "Received Response: {} files accepted",
                            accepted_diff.files.len()
                        );
                    }
                }
                node.handle_message_disk(msg, root, content_store)?
            }
            Event::Refresh(path_list) => node.refresh_requests(root, &path_list, content_store)?,
        };
        if let Some(response) = response {
            match &response {
                NodeMessage::Changes {
                    content_diff: _,
                    diff,
                } => {
                    info!("Sending Changes: {} files", diff.files.len());
                }
                NodeMessage::ChangesResponse { accepted_diff } => {
                    info!(
                        "Sending Response: {} files accepted",
                        accepted_diff.files.len()
                    );
                }
            }
            output.send(AnyNodeMessage::Regular(response))?;
        }
    }
    anyhow::Ok(())
}

fn debounce_watcher(
    path_list: Vec<RefreshRequest>,
    rx: &Receiver<Vec<RefreshRequest>>,
    ignore: &Regex,
) -> Result<Vec<RefreshRequest>, RecvError> {
    let mut paths = BTreeSet::new();
    paths.extend(path_list);
    // always break after 100ms
    let max_deadline = Instant::now() + Duration::from_millis(100);
    loop {
        // wait small duration for new event to come.
        let deadline = Instant::now() + Duration::from_millis(15);
        match rx.recv_deadline(deadline.min(max_deadline)) {
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
    Ok(paths
        .into_iter()
        .filter(|x| {
            let found = match x {
                RefreshRequest::FullRescan(path) => {
                    path.to_str().map_or(false, |x| !ignore.is_match(x))
                }
                RefreshRequest::Path(path) => path.to_str().map_or(false, |x| !ignore.is_match(x)),
            };
            found
        })
        .collect())
}

fn ssh_sync_command_with_retry(
    local_root: PathBuf,
    remote_host: String,
    remote_root: PathBuf,
    override_remote: bool,
    ignore: &str,
) -> Result<()> {
    const MAX_RETRIES: u32 = 10;
    const RETRY_DELAY: Duration = Duration::from_secs(5);
    const RESET_THRESHOLD: Duration = Duration::from_secs(60); // 1 minute

    let mut attempt = 1;
    loop {
        info!(
            "Attempting SSH connection (attempt {} of {})",
            attempt, MAX_RETRIES
        );

        let start_time = Instant::now();

        match ssh_sync_command(
            &local_root,
            &remote_host,
            &remote_root,
            override_remote,
            ignore,
        ) {
            Ok(_) => return Ok(()),
            Err(e) => {
                let connection_duration = start_time.elapsed();

                if connection_duration >= RESET_THRESHOLD {
                    debug!(
                        "Connection was active for more than 1 minute. Resetting attempt count."
                    );
                    attempt = 1;
                } else {
                    attempt += 1;
                }

                error!("SSH connection failed: {:?}", e);
                if attempt <= MAX_RETRIES {
                    info!("Retrying in {} seconds...", RETRY_DELAY.as_secs());
                    sleep(RETRY_DELAY);
                } else {
                    return Err(e.context("Max retries reached for SSH connection"));
                }
            }
        }
    }
}

fn ssh_sync_command(
    local_root: &Path,
    remote_host: &str,
    remote_root: &Path,
    override_remote: bool,
    ignore: &str,
) -> Result<()> {
    let local_root = local_root.canonicalize()?;
    let regex = Regex::new(ignore).unwrap();
    let mut cmd = Command::new("ssh");
    cmd.arg(&remote_host)
        .arg("fync")
        .arg("-i")
        .arg(&*shlex::try_quote(ignore).unwrap())
        .arg("run-stdio")
        .arg(&remote_root);
    if override_remote {
        cmd.arg("-o");
    }
    // Spawn the SSH process
    let mut child = cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).spawn()?;

    let child_stdin = child.stdin.take().expect("Failed to open child stdin");
    let child_stdout = child.stdout.take().expect("Failed to open child stdout");

    let result = run_node_with_io(
        &local_root,
        !override_remote,
        &regex,
        child_stdout,
        child_stdin,
    );

    // Wait for the child process to finish
    let status = child.wait()?;
    if !status.success() {
        bail!("SSH process exited with non-zero status");
    }

    result
}
