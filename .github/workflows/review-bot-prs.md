---
# review-bot-prs — daily Dependabot / pre-commit-ci PR safety review for dag-factory.
#
# This is a GitHub Agentic Workflow (github.com/githubnext/gh-aw). It is the source
# of truth; the runnable Action is the generated `review-bot-prs.lock.yml` sibling.
# After editing this file, run `gh aw compile` and commit BOTH files.
#
# Ported from the internal `review-bot-prs` Claude Code skill, scoped to dag-factory
# only (no cross-repo access). The agent runs read-only; its only routine action is
# posting one advisory comment per bot PR via safe-outputs (a "nothing to review" run
# logs a no-op to the Actions run summary, not an issue). gh-aw opens a tracking issue
# only if a run is incomplete or needs a missing tool. It cannot approve, merge, rebase,
# or push — those capabilities are simply not granted.

on:
  # Daily sweep, plus a manual "Run workflow" button.
  schedule: daily
  workflow_dispatch:
    inputs:
      aw_context:
        default: ""
        description: "Leave blank for manual runs. Internal JSON used only by gh-aw."
        required: false
        type: string

  # Pre-activation guard: skip the (paid) agent run unless there is at least one open
  # bot PR that does not already have this workflow's review comment.
  permissions:
    pull-requests: read
  steps:
    - id: check
      # Tolerate the non-zero exit below: a "nothing new to review" run should SKIP
      # the agent cleanly (run stays green), not mark the whole workflow failed. gh-aw
      # gates the agent on this step's outcome (check_result), so exit 1 = skip,
      # exit 0 = run.
      continue-on-error: true
      env:
        GH_TOKEN: ${{ github.token }}
      run: |
        set -o pipefail
        bot_prs=$(gh pr list --repo "$GITHUB_REPOSITORY" --state open --limit 200 --json number,author \
          --jq '.[] | select(.author.login=="app/dependabot" or .author.login=="app/pre-commit-ci") | .number')
        count=0
        unreviewed=0

        while IFS= read -r pr; do
          [ -n "$pr" ] || continue
          count=$((count + 1))
          if gh api --paginate "repos/$GITHUB_REPOSITORY/issues/$pr/comments" --jq '.[].body' \
            | grep -Eq 'gh-aw-workflow-id: review-bot-prs|Bot PR safety review'; then
            continue
          fi
          unreviewed=$((unreviewed + 1))
        done <<< "$bot_prs"

        echo "Open bot PRs: $count"
        echo "Unreviewed bot PRs: $unreviewed"
        [ "$unreviewed" -gt 0 ]

# Only run the agent when the guard above found at least one unreviewed open bot PR.
if: needs.pre_activation.outputs.check_result == 'success'

# GitHub Copilot engine (gh-aw default). No ANTHROPIC_API_KEY or PAT: the
# `copilot-requests: write` permission below routes inference through the org's
# Copilot subscription using the built-in Actions token.
engine: copilot

# The agent job is read-only — every GitHub write goes through the safe-outputs job.
# `copilot-requests: write` is the no-PAT Copilot auth path; the reads are what the
# review needs (PRs, issues, repo contents, and check/commit status for the CI gate).
permissions:
  contents: read
  pull-requests: read
  issues: read
  checks: read
  statuses: read
  actions: read
  copilot-requests: write

# This workflow only ever touches dag-factory itself.
tools:
  github:
    toolsets: [repos, pull_requests, issues]
    allowed-repos:
      - astronomer/dag-factory
    # Only read content at 'approved' integrity or higher (the default for public repos).
    # Same-repo bot PRs (Dependabot / pre-commit-ci, non-fork) qualify; injected content
    # from unapproved external accounts on the PR thread is filtered out before the agent.
    min-integrity: approved
  bash: ["gh:*", "git:*", "curl:*", "jq:*", "python3:*", "date", "grep", "sort", "head", "cat"]
  web-fetch:

# Egress firewall: GitHub plus the advisory / version sources the review needs.
network:
  allowed:
    - defaults
    - api.osv.dev
    - python      # ecosystem identifier (pypi.org + pythonhosted, etc.)

# The ONLY action this workflow can take: post one advisory comment per bot PR.
# Approve / merge / push are not declared, so the agent has no way to perform them.
safe-outputs:
  # The "nothing to review" steady state calls noop (see the prompt). Report it to the run
  # summary only — NOT as a new issue — so the daily schedule doesn't open issues each run.
  noop:
    report-as-issue: false
  add-comment:
    target: "*"        # comment on each bot PR it reviews (not just a triggering one)
    max: 20            # cap comments per run
    footer: false      # we append our own footer with a hidden workflow-id marker for idempotency

timeout-minutes: 20
---

# Bot PR safety review — dag-factory

You are reviewing open **bot dependency-bump PRs** in `${{ github.repository }}`
(authored by `app/dependabot` or `app/pre-commit-ci`). For each one, decide whether
it is safe to merge and post a short advisory comment.

**You never approve, merge, rebase, or push.** Your only action is posting one comment
per PR — merging is always a separate human decision.

## Steps

1. **List** open PRs authored by `app/dependabot` or `app/pre-commit-ci`
   (`gh pr list --repo ${{ github.repository }} --state open --limit 200 --json number,title,author,labels,createdAt,url`).
   **Skip any PR that already carries a comment from this workflow** (search the PR's
   comments for the `gh-aw-workflow-id: review-bot-prs` marker, or a prior "Bot PR safety
   review" comment) — stay idempotent across the daily runs.

2. For each remaining PR, answer **all** of these:

   - **Cooldown.** dag-factory configures a **7-day Dependabot cooldown** on all three
     ecosystems — `github-actions`, `pre-commit`, and `uv` (`.github/dependabot.yml`; the
     `uv` block landed in PR #765, merged 2026-06-09, with minor/patch grouping). A bump
     younger than 7 days **violates** cooldown **unless it is a security update** (security
     updates are exempt → cooldown **N/A**, not "violated"). Routine grouped `uv` (Python)
     bumps now open and are subject to the 7-day cooldown like the others — only treat a
     `uv` PR's cooldown as N/A when you can confirm it is a security update (advisory in the
     PR body / OSV).
   - **Security.** Does the bump fix a known CVE/GHSA, and does the *new* version carry
     any known advisory? Check OSV (`POST https://api.osv.dev/v1/query`) and the PR body.
   - **Risk / blast radius.** Direct vs transitive vs dev-only dependency, breaking
     changes, and whether CI actually exercises the change.
   - **CI gate.** Are all required checks green on the head SHA? Flag any check that is
     pending, failing, or **awaiting maintainer authorization** ("Approve and run") — a
     "green" PR with half its checks un-triggered is not validated.
     **dag-factory nuance:** the `Static-Check` job runs
     `pre-commit run --files dagfactory/*`, so it exercises
     `ruff` / `black` / `codespell` but **NOT** `markdownlint` or
     `markdown-link-check` (no `.md` file is in the `--files` list). Green CI does **not**
     prove a markdownlint / markdown-link-check bump is clean — say so explicitly.
   - **Tag integrity (github-actions bumps only).** For a SHA-pinned action
     (`uses: owner/action@<sha>  # vX`), verify the new commit SHA in the diff *actually*
     resolves from the tag named in the trailing comment
     (`gh api repos/<owner>/<action>/git/ref/tags/<tag>`, dereferencing annotated tags,
     trying the `v` prefix both ways). A SHA that does not match the tag — or a
     tag-not-found — is a supply-chain red flag → force **⚠️ Review carefully**.

3. **Post one comment per PR** with a clear verdict and a one-line reason per axis:
   - **✅ Safe to merge**
   - **⏸️ Hold** (e.g. cooldown not yet elapsed)
   - **⚠️ Review carefully — do not merge** (any red flag above)

   Keep it public-safe: no customer data, internal hostnames, or private links.

4. **End every posted comment** with this footer (fill the timestamp with the current
   UTC time, `date -u +"%Y-%m-%d %H:%M UTC"`). Keep the hidden marker exactly as written;
   the pre-activation guard uses it to avoid duplicate comments across runs:

   ```markdown
   <!-- gh-aw-workflow-id: review-bot-prs -->
   ---
   *This review comment was generated by an agent. It is advisory only and does not
   approve or merge the PR. Reviewed on <YYYY-MM-DD HH:MM UTC>.*
   ```

**If there is nothing to comment on** — every open bot PR was skipped because it is
already reviewed (the normal steady state after the first run), or no unreviewed bot PR
remains — you **MUST** call the `noop` tool with a short message
(`{"noop": {"message": "No unreviewed bot PRs"}}`). A safe-outputs run that finishes
without calling any tool fails silently with no usable output; `noop` is how you report
"no action needed" (it goes to the run summary, not a new issue or comment).

If you cannot complete an axis (e.g. a tool or network host is unavailable), say so in
the comment rather than guessing.
