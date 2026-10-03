# Changi

Changi is a local terminal companion for one engineer working in one directory. Run `changi` to start or reuse a small background daemon and open a monitor. Press `q` to leave the monitor; the daemon keeps serving that workspace.

The monitor shows three separate views:

| View | What it means |
| --- | --- |
| Record | An event was durably written to a local SQLite ledger. |
| Signal | A recorded request for attention. |
| Work | An explicit work receipt with the required execution evidence. |

A Signal never counts as completed Work on its own. Changi does not launch agents, run commands, read terminal scrollback, or contact a network service.

## Install

On Linux, with Node.js 18+ and Python 3.10+ installed:

```sh
npm install --global @agentbus/changi
```

Run `changi` from the directory you want to monitor. The npm package provides the command and ships the Python runtime. Python is a runtime prerequisite; the package does not download or install Python. To select a different interpreter, set `CHANGI_PYTHON` to its executable path.

You can also use a source checkout with `python3 changi`.

## Commands

```sh
changi                         # start or reuse the daemon; open the monitor
changi status --json           # per-plane status for scripts
changi emit demo.signal '{"changi":{"kind":"signal","state":"pending"}}'
changi log --limit 20          # recent records
changi stop                    # stop this workspace's daemon
changi list-all                # show registered workspaces
changi stop --all              # preview; add --yes to stop all listed daemons
changi --version               # installed package version
```

`q` or Ctrl-C detaches from the monitor. Without a TTY, `changi` prints JSON status and leaves the daemon running. Idle daemons shut down after four hours without a client request; `CHANGI_IDLE_SECONDS` can set a positive alternate timeout.

Changi stores its ledger, socket, PID, and lock inside the selected workspace's `.changi/` directory. It also stores a small workspace registry at `${XDG_CONFIG_HOME:-~/.config}/changi/workspaces.json`. These files are owner-only. Stopping the daemon leaves `events.db` intact.

Changi never edits `.gitignore` or Git configuration during startup. If `.changi/` is unignored, it prints an advisory. To add an ignore rule explicitly, run `changi init --git-ignore` or `changi init --exclude` to preview the change, then rerun with `--yes` to apply it. In an unignored repository, `.changi/` will appear as an untracked directory.

## Develop and test

```sh
python3 -m unittest discover -s tests -v
npm pack --dry-run
```

The daemon uses a Unix domain socket. Its lifecycle tests require a Linux host that permits binding one; restricted sandboxes skip those cases and cannot independently certify the transport. Changi is a separate project and does not depend on the AgentBus server or Python package.

## Release

The npm package is published from a version tag after independent Pi QA. The first publication requires the package owner's npm authentication. After that, the owner can configure `onicarps/changi` and workflow `publish.yml` as an npm trusted publisher, so subsequent tagged releases use GitHub OIDC. The tag and `package.json` version must agree.

MIT licensed. The initial runtime was developed as the AgentBus Changi prototype; its provenance and independent prototype QA are recorded in the OKF Changi initiative.
