# AGENTS.md

This file is for AI coding agents (Claude Code, Cursor, Codex, …) working on **dag-factory**, a
Python library for [Apache Airflow®](https://airflow.apache.org/) that builds DAGs from YAML
configuration files.

## Environment Setup

- Install [`uv`](https://docs.astral.sh/uv/) and [`hatch`](https://hatch.pypa.io/) — `uv` manages the local virtualenv, `hatch` runs the test/docs matrix.
- Set up the dev environment with `uv sync --dev`. This creates `.venv/` with the right Python and all dependencies from `uv.lock`. `make setup` is an alternative.
- Activate the venv: `source .venv/bin/activate` (or `source venv/bin/activate` if you used `make setup`).
- Install pre-commit hooks once: `pre-commit install`.
- To run example DAGs locally, export:
  - `AIRFLOW_HOME=$(pwd)/dev`
  - `AIRFLOW__CORE__LOAD_EXAMPLES=false`
  - `CONFIG_ROOT_DIR=$AIRFLOW_HOME/dags`

## Commands

| Task | Command |
| --- | --- |
| Unit tests (one matrix cell) | `hatch run tests.py3.10-2.9:test` |
| Unit tests with coverage | `hatch run tests.py3.10-2.9:test-cov` |
| Unit tests across the full matrix | `hatch run tests:test-cov` |
| Integration tests setup | `hatch run tests.py3.11-2.9:test-integration-setup` |
| Integration tests | `hatch run tests.py3.11-2.9:test-integration` |
| Static checks (ruff, black, codespell, …) | `pre-commit run --all-files` |
| Build wheel + sdist | `uv build --wheel --sdist` (or `make build-whl`) |
| Local Airflow via Astro CLI | `make docker-run` / `make docker-stop` |
| Docs (build + serve locally) | `hatch run docs:dev` |
| Docs (strict build) | `hatch run docs:build` |

Notes:

- The Airflow/Python matrix is in `pyproject.toml` under `[[tool.hatch.envs.tests.matrix]]`. Picking a `py<py>-<af>` cell that is not in the matrix will fail.
- Integration tests need `AIRFLOW_HOME`, `CONFIG_ROOT_DIR`, and `PYTHONPATH` pointing at `dev/` and `dev/dags`. See `docs/contributing/howto.md` for the full export block.
- Integration tests are selected by the `integration` pytest marker (`-m integration`); `tests/test_example_dags.py` is excluded from the unit run.
- `scripts/test/pre-install-airflow.sh` pulls Airflow constraints for the requested version — don't bypass it when reproducing CI failures locally.

## Repository Structure

```
dag-factory/
├── dagfactory/         # Library source
│   ├── dagfactory.py   # Public entry points (load_yaml_dags)
│   ├── dagbuilder.py   # Translates YAML config into Airflow DAG/Task objects
│   ├── parsers.py      # Schedule/parameter parsing helpers
│   ├── _yaml.py        # YAML loading (safe loader, custom tags)
│   ├── constants.py    # Shared constants
│   ├── exceptions.py   # Library-specific exceptions
│   ├── settings.py     # Runtime settings / env var handling
│   ├── telemetry.py    # Anonymous usage telemetry (opt-out respected)
│   ├── utils.py        # Misc helpers
│   ├── listeners/      # Airflow listener integrations
│   └── plugin/         # Airflow plugin entry point (DagFactoryPlugin)
├── tests/              # Pytest suite, mirrors `dagfactory/`
│   ├── fixtures/       # YAML fixtures used by unit tests
│   └── fixtures_without_default_yaml/  # Fixtures for tests that omit a default YAML
├── dev/                # Local Astro/Airflow sandbox (Dockerfile, dags/, logs/)
│   └── dags/           # Example DAGs used locally and by tests/test_example_dags.py
├── examples/dags/      # Example YAML DAG configs included in the source tree
├── docs/               # mkdocs-material site
├── scripts/            # Test, doc, and release helpers
├── pyproject.toml      # Build + tool config (ruff, black, hatch, uv)
└── uv.lock             # Locked dependency graph — regenerated via `uv lock`
```

The library is single-package (`dagfactory`); there is no monorepo or workspace split. `dev/` and `examples/` are not packaged into the wheel (see `[tool.hatch.build.targets.wheel]`).

## Architecture Boundaries

dag-factory is a thin authoring layer that runs *inside* an Airflow deployment. Keep these responsibilities separate:

1. **YAML loading** (`_yaml.py`, `dagfactory.py`) reads config from disk or a Python dict and applies defaults from `defaults.yml`.
2. **DAG building** (`dagbuilder.py`) maps the parsed YAML onto Airflow primitives (DAG, Operator, TaskGroup, mapped tasks). Airflow-version compatibility shims live here — guard with `try/except ImportError` rather than version checks (see the `airflow.sdk.definitions.dag` fallback in `dagfactory.py`).
3. **Parsing helpers** (`parsers.py`) turn YAML strings into Airflow types (schedules, timedeltas, callbacks, Python callables).
4. **CLI** (`__main__.py`, the `dagfactory` Typer console script) is for operator commands; it should not import from runtime listener code.
5. **Telemetry** (`telemetry.py`) must stay opt-out and must never block DAG parsing if the network is down. Errors are swallowed by design.

Don't import Airflow at module top-level in code that may run before Airflow is initialized; prefer local imports or guarded `try/except ImportError`. dag-factory must keep working on both Airflow 2.9+ and Airflow 3.x.

## Security Model

Vulnerability reports go to `oss_security@astronomer.io` (see `SECURITY.md`). Don't file security issues on GitHub.

## Coding Standards

- Formatting and linting are enforced via `pre-commit`:
  - `black` and `ruff`, both with `line-length = 120`. Ruff rule selection is `["C901", "D300", "I", "F"]`; isort `known-first-party = ["dagfactory", "tests"]`.
  - `codespell`, `markdownlint`, `markdown-link-check`, plus checks for large files, merge conflicts, private keys, and AWS credentials.
  - `uv-lock` keeps `uv.lock` in sync — re-run `uv lock` after editing dependencies.
- All source files are Apache-2.0 licensed (see `LICENSE`); don't add files under a different license without maintainer sign-off.
- Raise library-specific exceptions from `dagfactory/exceptions.py` (e.g. `DagFactoryException`, `DagFactoryConfigException`) rather than bare `Exception` or generic `RuntimeError`.
- Public API is whatever `dagfactory/__init__.py` re-exports (`__all__`). Treat it as a contract — additions are fine, renames/removals need a deprecation cycle and a `CHANGELOG.md` entry.
- For Airflow version compatibility, prefer `try/except ImportError` over parsing `airflow.__version__`.

## Testing Standards

- Tests live under `tests/` and mirror the package layout (`tests/test_<module>.py`).
- New behavior needs a unit test. Reproduce bugs with a failing test before fixing.
- Use existing YAML fixtures in `tests/fixtures/` and `tests/fixtures_without_default_yaml/` instead of inlining large strings.
- Mark integration tests with `@pytest.mark.integration` so they're skipped in the unit run; mark callback tests with `@pytest.mark.callbacks`. Both markers are registered in `pyproject.toml`.
- Example-DAG validation lives in `tests/test_example_dags.py` and is skipped in the unit run. Run it explicitly when touching `examples/dags/` or `dev/dags/`.
- When debugging cross-version issues, run the matrix cell that matches the bug.
- Don't depend on network calls or the local `dev/airflow.db` from unit tests.

## Commits and PRs

- Branch off `main`.
- Keep commits focused; align the PR title with the change and link the GitHub issue when one exists.
- For features that change YAML behavior, update the relevant page under `docs/` in the same PR.
- Before pushing, always rebase your branch onto the latest target branch (usually `main`) to avoid merge conflicts and ensure CI runs against up-to-date code:

  ```bash
  git fetch <upstream-remote> <target_branch>
  git rebase <upstream-remote>/<target_branch>
  ```

- Run `pre-commit run --all-files` and at least one unit-test matrix cell before requesting review.
- CI runs the full matrix (`.github/workflows/cicd.yaml`). Wait for green before merging.
- A maintainer must approve before merge — don't self-merge.

## Boundaries

Ask first:

- Bumping the minimum Airflow version or dropping a Python version from the matrix.
- Changing telemetry behavior, scope, or default opt-in/out posture.
- Modifying the public API in `dagfactory/__init__.py` or the `dagfactory` CLI commands.
- Editing `SECURITY.md`, `LICENSE`, `PRIVACY_NOTICE.md`, or `CODEOWNERS`.
- Touching release tooling (`scripts/docs_deploy.py`, `scripts/verify_tag_and_version.py`, GitHub release workflows).

Never:

- Commit secrets, tokens, or credentials. The pre-commit `detect-private-key` and `detect-aws-credentials` hooks are there for a reason — don't bypass them.
- Hand-edit `uv.lock`. Regenerate it with `uv lock` (or `uv sync`) and commit the result.
- `git push --force` against `main` or release branches, or rewrite published history.
- Publish to PyPI from a developer machine outside the documented release flow (`hatch version …` → GitHub Release → CI publish).
- Skip pre-commit hooks (`--no-verify`) to land otherwise-failing changes.

## References

- [Quickstart — Astro CLI](https://astronomer.github.io/dag-factory/latest/getting-started/quick-start-astro-cli/)
- [Quickstart — Airflow Standalone](https://astronomer.github.io/dag-factory/latest/getting-started/quick-start-airflow-standalone/)
- [Contributing Guide](https://astronomer.github.io/dag-factory/latest/contributing/howto/) (mirrored at `docs/contributing/howto.md`)
- [Code of Conduct](docs/contributing/code_of_conduct.md)
- [Roles](docs/contributing/roles.md)
- [Migration Guide (1.0)](https://astronomer.github.io/dag-factory/latest/migration_guide/)
- [Security Policy](SECURITY.md)
- [Privacy Notice](PRIVACY_NOTICE.md)
- [CHANGELOG](CHANGELOG.md)
