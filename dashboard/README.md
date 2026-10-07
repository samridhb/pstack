# pstack Flow dashboard

Live view of agent flow for any Claude Code session: spawn graph, per-agent
timeline, event feed, and which agents/rules/skills were touched.

    ~/.claude/pstack-dashboard/run.sh        # http://127.0.0.1:8765

- Reads `~/.claude/projects/*/<session>.jsonl` and `<session>/subagents/*.jsonl` (read-only, polled every 0.5s).
- Binds to 127.0.0.1 only and rejects non-localhost Host headers. Transcripts contain prompts and tool output, so keep it local.
- "follow latest" tracks the most recently active session; untick it to pin one.
- Spawns the harness rejected (e.g. agent not registered yet) show as red dashed nodes.
- Click a node to filter the feed and dim other lanes in the timeline. Click a feed row to expand its detail.
- Stdlib Python only, no build step. `--hours N` changes how far back sessions are tracked (default 12).
