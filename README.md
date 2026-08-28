# slurmboard

A lightweight, dependency-free web dashboard for Slurm clusters.

Run it directly on a Slurm login node, or launch it from your Mac through an SSH tunnel — no extra packages, just Python 3 stdlib.

![slurmboard screenshot](assets/screenshot.png)

## Features

- **Three-column layout** — cluster summary, partition table, and personal job history side by side; columns are draggable to resize
- **Cluster summary** — idle/total for nodes, CPUs, memory, GPUs with green progress bars (green = more idle = better)
- **GPU breakdown by type** — H100 / A100 / V100 / … with idle% bars
- **Partition table**
  - Multi-column sort (click header = primary, Shift+click = secondary)
  - Filter by minimum VRAM and/or "idle GPUs only"
  - Lazy per-partition node loading and async refresh (↻) without losing expand/sort state
  - Running / pending jobs load only when requested; click to expand the inline list
  - Click any row to load and expand its node details (CPU, memory, GPU idle/total, VRAM)
- **Job detail page** at `/job/<id>` — full `scontrol` info, linked from job lists
- **My Jobs panel** — persistent job history (`jobs_history.json`), marks finished jobs as DONE; sortable by state / ID / time / date; shows submit time
- **All progress bars show idle ratio** — green bar = available resources

## Installation

```bash
git clone https://github.com/zhangdoudou/slurmboard.git
cd slurmboard
chmod +x slurmboard.py
```

Optionally add it to your `PATH` for convenience:

```bash
ln -s "$PWD/slurmboard.py" ~/.local/bin/slurmboard
```

No `pip install` needed — pure Python stdlib.

## Requirements

- Python ≥ 3.7 (stdlib only — no pip installs)
- Running on a node with `sinfo`, `scontrol`, `squeue` in `$PATH` (Slurm login or submit node)

## Usage

### Launch from your Mac

If your clusters are already configured as named hosts in `~/.ssh/config`, start the local launcher:

```bash
./slurmboard.py --launcher
```

Your browser normally opens `http://127.0.0.1:65432` (or another free local port if that one is already occupied). The page lists concrete `Host` aliases from your SSH config; click **Connect**, then **Open dashboard**. For example, this entry appears as a `roihu-gpu` button:

```ssh-config
Host roihu-gpu
    HostName login.example.org
    User my-user
    IdentityFile ~/.ssh/id_ed25519
```

The launcher does all of the following for you:

- uses the host's existing SSH settings, including `ProxyJump`, identity files, and included config files
- sends the current `slurmboard.py` to the remote host, so no separate remote installation is needed
- starts the remote dashboard on loopback and creates the local tunnel
- lets you pin frequently used hosts to the top; pins persist between launcher runs
- stops the remote dashboard and tunnel when you click **Stop** or quit the launcher with `Ctrl+C`

SSH authentication must work non-interactively (for example, with macOS Keychain or `ssh-agent`). The launcher automatically chooses a high, temporary port for each connection, so it does not conflict with an already-running dashboard. If you specifically need a fixed remote port, set one explicitly:

```bash
./slurmboard.py --launcher --remote-port 65435
```

Wildcard-only entries such as `Host *` are not shown because they are SSH defaults rather than connectable host names.

The dashboard refreshes its compact cluster summary every minute by default. Use the **Auto refresh** menu to choose manual refresh, 15 or 30 seconds, or 1, 2, or 5 minutes. The choice is remembered across launcher ports. Automatic refresh skips hidden browser tabs and refreshes the active queue only after you have chosen to load it; job history remains manual because it is a heavier query.

### Run directly on a login node

```bash
# default: bind 0.0.0.0:8000
./slurmboard.py

# custom port / bind address
./slurmboard.py --port 9000 --host 127.0.0.1
```

Open `http://<login-node>:8000` in your browser. Use the ↻ buttons to refresh data without a full page reload.

## How it works

The first page load makes one compact, partition-oriented query:

```
sinfo -h -o "%P|%a|%l|..."  # aggregate partition/resource summary
```

Additional commands are scoped and loaded asynchronously:

```
sinfo -N -e -p <partition>   # exact nodes only after expanding a partition
squeue -p <partition>        # jobs only after clicking “load jobs”
squeue -u <user>             # personal active queue
sacct -u <user>              # personal seven-day history
```

Results are cached for 10–60 seconds to absorb repeated clicks and concurrent requests are serialized to avoid bursts against the Slurm controller. The frontend is vanilla JS — no framework or build step. Only the compact cluster summary polls at the selected automatic-refresh interval (one minute by default); node details, partition jobs, and job history remain request-driven.

Job history is persisted to `jobs_history.json` (same directory as the script, gitignored) so completed jobs remain visible in My Jobs for 7 days.

## Typical workflow

1. You need N GPUs with at least X GB VRAM.
2. Open slurmboard, filter by **Min VRAM**, sort by **GPU (idle/total) ↓**.
3. Check **Jobs (run/pend)** to gauge queue pressure.
4. Click a partition row to expand its nodes and pick the least loaded one.
5. Monitor your submitted jobs in the **My Jobs** panel on the right.

## Inspiration

Motivated by [slurmmanager](https://github.com/paulgavrikov/slurmmanager); built to run without SSH access to compute nodes.

## License

MIT © 2026 zhangd — see [LICENSE](LICENSE).
