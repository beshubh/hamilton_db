---
name: launch-claude-tab
description: Launch a new Claude Code session in a new tab of the user's terminal multiplexer (Rex). Use when asked to start, open, or spawn another Claude session/tab.
---

# Launch Claude Code in a new tab

The user's multiplexer is Rex (`TERM_PROGRAM=rex`, `REX_SESSION` set). In Rex a tab is a *window*; `rex run` opens one, `rex split` makes a split (not wanted).

1. Open a new window in the current Rex session running `claude` in the repo:

   ```sh
   rex run -s "$REX_SESSION" -c /Users/shubham/code/hamilton_db --label claude --focus=false --json -- claude
   ```

   - To hand it a first prompt: `-- claude "<prompt>"`.
   - Drop `--focus=false` if the user wants to be switched to the new tab.

2. Verify it started, using the `block_ids[0]` from the JSON output:

   ```sh
   rex capture -b <block_id> --trim | tail -12
   ```

   Expect the Claude Code banner and the `❯` prompt.

3. Tell the user it's open and in which tab. Afterwards, `rex send -b <block_id>` types into it and `rex capture` reads it.

If `REX_SESSION` is unset, the session isn't in Rex: check `TMUX` / `ZELLIJ` and use that multiplexer's new-window command instead.
