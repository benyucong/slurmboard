# Slurmboard.app — macOS client

Slurmboard.app is a native macOS shell for monitoring Slurm clusters and
standalone Linux servers. Hosts, tabs, SFTP, and terminal sessions use the
SwiftUI app, while each dashboard is the bundled Python page displayed in a
`WKWebView` through an SSH tunnel.

The app maintains its own copy of [`slurmboard.py`](slurmboard.py). That file is
packaged into the app at build time and is updated manually with the app source;
the app never downloads dashboard code at runtime.

## How it works

When you open a dashboard, the app:

1. Reads the bundled `slurmboard.py`.
2. Starts the system `/usr/bin/ssh` client with a local port forward.
3. Streams the Python source over SSH standard input; it does not install or
   permanently upload a file to the login node.
4. Runs the streamed source with the remote `python3` interpreter, bound only to
   `127.0.0.1` on a temporary remote port.
5. Waits for the dashboard health endpoint and opens the forwarded local URL in
   a `WKWebView` tab.
6. Terminates the remote process and SSH tunnel when the dashboard is closed or
   the app quits.

```text
bundled slurmboard.py
        │ stdin
        ▼
macOS app ── /usr/bin/ssh ── login node: python3 -
        │                         │
        └── local port forward ◀──┘
                    │
                    ▼
                 WKWebView
```

## Features

- Host cards imported from `~/.ssh/config` or added manually.
- Manual hosts can be entered as a complete SSH command or as connection
  fields: hostname, username, port, identity file, `ProxyJump`, and extra SSH
  arguments.
- Host cards can be edited or removed; the add and edit screens share the same
  connection form.
- Optional destination and ProxyJump passwords are stored separately in the
  user's macOS Keychain, not in `hosts.json`, command arguments, logs, or the
  repository.
- Failed dashboard connections show a password field so password-only hosts
  can be retried immediately, with optional Keychain saving.
- One monitor tab per host, rendered by the bundled web UI.
- Automatic mode selection: Slurm hosts show partitions, nodes, jobs, and
  quotas; non-Slurm servers show CPU, memory, load, disks, NVIDIA GPU/VRAM, GPU
  processes, and the connected user's top processes.
- An embedded SwiftTerm terminal and native SFTP tabs.
- Dashboard lists initially render at most 100 rows and append more rows when
  scrolled to the bottom. There are no pagination or “load more” controls.
- Partition job counts, GPU allocation summaries, Active Queue, and seven-day
  History load automatically. Large node and job detail lists remain
  request-driven and incrementally rendered.
- Storage quota discovery is optional. If the cluster has no supported quota
  command, the Storage Quota section is hidden.
- BSC systems are detected through `bsc_quota`, including user/group GPFS
  space usage and file counts even when the standard `quota` command fails.

## Requirements

### Local Mac

- macOS 14 or later.
- Xcode with Swift 5.9 or later and the Metal compiler tools for SwiftTerm's
  shaders. Command Line Tools alone may not include the required `metal` compiler.
- System OpenSSH (`/usr/bin/ssh`, included with macOS).

Check the selected developer tools:

```bash
xcode-select -p
swift --version
xcrun --find metal
```

### Remote host

- SSH access to a Slurm login node or standalone Linux server.
- Python 3.7 or later available as `python3` or `python3.7`–`python3.13`.
- For Slurm mode, `sinfo`, `scontrol`, and `squeue` must be available in `PATH`;
  `sacct` is required for job history and quota commands are optional.
- For standalone server mode, Linux `/proc`, `df`, and `ps` provide system data;
  `nvidia-smi` is optional and enables NVIDIA GPU data.

The remote account does not need Slurmboard installed. The app streams its
bundled source for every dashboard connection.

## Build from source

Clone the repository and build a release app bundle:

```bash
git clone https://github.com/zhangdoudou/slurmboard.git
cd slurmboard/SlurmboardApp
./build_app.sh --release
open Slurmboard.app
```

`build_app.sh` performs a Swift release build, assembles
`Slurmboard.app/Contents`, generates the `.icns` icon set from
`Resources/AppIcon.png`, embeds `slurmboard.py` and Swift package resource
bundles, and applies an ad-hoc local signature. Swift package dependencies
require internet access on the Mac during the first build; monitoring does
not require internet access on the remote host.

For a faster development build:

```bash
./build_app.sh --debug
open Slurmboard.app
```

You can also run the Swift package directly during development:

```bash
swift run
```

## Add or import a host

You can import concrete aliases from `~/.ssh/config`:

```ssh-config
Host lumi
    HostName lumi.csc.fi
    User my-user
    IdentityFile ~/.ssh/id_ed25519
    ProxyJump my-bastion
```

Wildcard-only entries such as `Host *` are defaults and are not shown as host
cards.

Alternatively, click **Add Host** and choose one of these input methods:

- **SSH Command** — for example, `ssh -J user@jump.example.org user@login.example.org`.
- **Connection Fields** — enter the host, user, port, identity file,
  `ProxyJump`, extra arguments, and optional password separately.

When editing a host, leaving the password field empty preserves the saved
Keychain password. Password entry is available for both complete SSH commands
and connection-field hosts. Use the clear-password control to delete it.

If a dashboard connection fails because SSH needs a password, enter the
destination password, the jump-host password, or both directly on the
connection screen and click **Connect with Password**. Leave **Save entered
passwords in macOS Keychain** enabled to reuse them for later connections. The
askpass helper matches the jump hostname in OpenSSH's prompt so a ProxyJump
password is not sent in response to a destination-host password prompt.

Before connecting for the first time, accept the remote host key in Terminal if
your SSH policy does not allow an interactive host-key prompt inside the app:

```bash
ssh lumi
```

## Local data and security

- Host definitions are stored in
  `~/Library/Application Support/Slurmboard/hosts.json`.
- Passwords are stored as macOS Keychain generic-password items.
- For password-authenticated dashboard SSH, a permission-restricted temporary
  askpass helper is created and removed after authentication.
- Private keys and passwords are not added to the repository or app bundle.
- The dashboard HTTP server listens on remote loopback, and its local forwarded
  endpoint listens on `127.0.0.1`.

## Project layout

```text
SlurmboardApp/
  Package.swift
  Info.plist
  build_app.sh
  Resources/AppIcon.png        1024 px application icon source
  scripts/make_icns.py          fallback ICNS packer for affected macOS toolchains
  slurmboard.py                 embedded dashboard backend and web UI
  Sources/SlurmboardApp/
    SlurmboardApp.swift
    Models/SSHConfig.swift
    Services/
      ConnectionManager.swift
      CredentialStore.swift     macOS Keychain integration
      DashboardService.swift    SSH streaming, tunnel, and lifecycle
    Views/
      HostPickerView.swift      host cards and shared add/edit form
      ClusterWindowView.swift   WKWebView dashboard tab
      TerminalView.swift
      SFTPView.swift
```

Some earlier native dashboard models and views remain in the source tree, but
the current Slurm dashboard path uses `DashboardService` and `WKWebView`.

## Troubleshooting

### The build cannot find `metal`

SwiftTerm includes Metal shaders. Select a full Xcode installation with its
Metal compiler tools, then check that `xcrun --find metal` succeeds before
rebuilding. Installing Command Line Tools alone may not be sufficient.

### `swift build` reports an SDK/compiler mismatch

Reinstall or select a matching Xcode/Command Line Tools installation, then
confirm the active toolchain:

```bash
xcode-select -p
swift --version
```

### The dashboard cannot connect

First test the same host with the system SSH client:

```bash
ssh <host-alias>
```

Then verify `python3` is available. For a Slurm dashboard, also verify `sinfo`,
`scontrol`, and `squeue` are in `PATH`. A standalone server does not need those
commands or internet access. SSH options from the saved host, including
identity files and `ProxyJump`, are passed to `/usr/bin/ssh`.

### Storage Quota is absent

This is expected when the cluster does not expose a supported quota command.
The rest of the dashboard remains available.
