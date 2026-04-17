# DVID Repo Guidance

- Read `CLAUDE.md` in this repository before substantial work. It is the primary repo context file for DVID architecture, data types, testing expectations, and environment constraints.
- Treat `CLAUDE.md` as repo-specific guidance layered on top of broader Codex instructions and apply it unless a higher-priority instruction overrides it.
- During development, prefer focused tests for the changed package or subsystem to keep iteration fast.
- For broader validation or when explicitly requested, run `make test`.
- If full-repo validation was not run, say so explicitly in the final response.
