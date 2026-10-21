# EnergyPulse: agent rules

Instructions for coding agents working in this repo; a human reader wants `README.md`. Read
`README.md` for how it works, `RUNBOOK.md` for procedures, `TASK.md` for what is open. The
owner's global agent rules apply on top of these.

- Public repo: no personal data, account numbers, keys or third-party names in any file.
- Real-time commits only; never backdate or set `GIT_AUTHOR_DATE`. No AI attribution.
- Release steps: `RUNBOOK.md` §Release.
- The demand data is simulated and the README says so; never describe it as real grid data.
- Dev tools are an extra, not a dependency group: a test or lint that "is not installed" means
  `uv sync --all-extras`, not a dependency change.
