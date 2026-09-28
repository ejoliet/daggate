# daggate — Airflow DAG Production Readiness Kit

> A zero-dependency static gate that blocks unproduction-ready Airflow DAGs in CI — plus the review checklist your team stops arguing about.

## What this is

`daggate` runs 16 policy checks against your DAG files **without installing Airflow**. Pure Python standard library, AST-based, one file. It complements — does not replace — `ruff`'s AIR rules: ruff covers Airflow 3 migration syntax; daggate covers production policy (retries, catchup, secrets, top-level code, alerting, timeouts).

**Kit contents**

| Item | Path |
|------|------|
| The linter (single file) | `daggate.py` |
| Test suite (30 tests) | `tests/` |
| Good + bad example DAGs | `examples/dags/` |
| Production readiness checklist (human review) | `docs/PRODUCTION-READINESS-CHECKLIST.md` |
| GitHub Actions workflow | `ci/github-actions-daggate.yml` |
| Jenkins pipeline stage | `ci/jenkins-stage.groovy` |
| pre-commit hook config | `ci/pre-commit-config.yaml` |
| FAQ + troubleshooting | `docs/` |

## Quick Start

Requires Python 3.10+. No pip installs needed.

```bash
# 1. Copy daggate.py anywhere (or keep it in your repo root)
# 2. Run it against your dags folder
python daggate.py dags/

# CI-friendly variants
python daggate.py dags/ --format github          # GitHub annotations
python daggate.py dags/ --format json            # machine-readable
python daggate.py dags/ --strict                 # warnings fail too
python daggate.py dags/ --ignore DG105,DG106     # tune to your policy
```

Exit codes: `0` clean, `1` errors found (or warnings with `--strict`), `2` parse/usage failure.

Optional pip install (adds a `daggate` command):

```bash
pip install .
daggate dags/
```

## Rules

| Code | Sev | Check |
|------|-----|-------|
| DG101 | error | `catchup` not set explicitly |
| DG102 | error | No `retries` in `default_args` |
| DG103 | warn | `retries` without `retry_delay` |
| DG104 | warn | Missing `owner` |
| DG105 | warn | No `tags` |
| DG106 | warn | No `doc_md`/`description` |
| DG107 | error | No explicit `schedule` (use `None` for manual-only) |
| DG201 | error | `Variable.get()` at module top level |
| DG202 | error | Network/DB/file I/O at module top level |
| DG203 | error | Dynamic `start_date` (`now()`, `days_ago()`) |
| DG204 | warn | Naive (timezone-less) `start_date` |
| DG301 | error | Hardcoded secret in assignment |
| DG302 | error | Credentials embedded in URL literal |
| DG401 | warn | No `dagrun_timeout` |
| DG402 | warn | No failure alerting configured |
| DG403 | warn | `depends_on_past=True` without `max_active_runs=1` |

Every rule maps to a section of the human-review checklist in `docs/PRODUCTION-READINESS-CHECKLIST.md`, which covers what static analysis cannot: idempotency of task logic, backfill safety, data quality gates, and on-call runbooks.

## CI integration

GitHub Actions: copy `ci/github-actions-daggate.yml` to `.github/workflows/`.
Jenkins: paste `ci/jenkins-stage.groovy` into your pipeline.
pre-commit: merge `ci/pre-commit-config.yaml` into your `.pre-commit-config.yaml`.

## Compatibility

- Python 3.10 – 3.13. Zero runtime dependencies.
- Airflow 2.x and 3.x DAG files (classic `DAG(...)`, `with DAG(...)`, and `@dag` TaskFlow).
- Works on files that would fail to import (missing providers, no Airflow installed) — it never executes your code.

## Known limits

Static analysis is heuristic. daggate cannot follow `**kwargs` expansion, DAG-factory indirection through function calls, or values loaded from YAML/JSON. For dynamically generated DAGs, run daggate against the generator's template output or rely on the checklist. Suppress a rule per-repo with `--ignore`.

## Development

```bash
pip install pytest
python -m pytest tests/ -v
```

## Support & license

Single-team commercial license — see `LICENSE.md`. Support is limited to defect reports for 12 months from purchase (see `docs/FAQ.md`). No custom rule development is included.

## Changelog

See `CHANGELOG.md`.
