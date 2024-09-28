# Fync

Fync is a simple file synchronization tool for two systems.
It is specialized for syncing source code (i.e. lots of small text files).

## Features

- Efficient file change detection
- Bi-directional synchronization
- SSH synchronization support

## Commands

Fync supports the following commands:

1. `ssh-sync`: Synchronize files with a remote system using SSH
   ```
   fync ssh-sync <local_root> <remote_host> <remote_root>
   ```

2. `sync`: Synchronize files between two local directories
   ```
   fync sync <source> <destination>
   ```

3. `run-stdio`: Run Fync in stdio mode (used internally)
   ```
   fync run-stdio <root> [-o]
   ```

4. `watch`: Watch a directory for changes (for debugging)
   ```
   fync watch <directory>
   ```

Use the `-h` or `--help` flag with any command for more information.
