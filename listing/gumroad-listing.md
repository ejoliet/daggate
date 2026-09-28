# Gumroad Listing — daggate

**Title (search-optimized):**
daggate — Airflow DAG Production Readiness Kit (CI linter + checklist, 16 rules, zero dependencies)

**Category:** Software Development > Developer Tools
**Price:** $39 (launch), $49 after first 25 sales. Team license $149 (up to 25 seats, included by default in LICENSE terms — position $39 as "single team up to 25 engineers" OR split: $39 = 5 seats, $149 = 25 seats. Decide before publish; keep one tier at launch: **$39, single team**.)

**One-sentence promise:**
Block unproduction-ready Airflow DAGs in CI in under 5 minutes — no Airflow install, no dependencies, no arguing in code review.

**Description:**

Your data pipeline broke at 3 AM because a DAG shipped with `retries=0`, `catchup` unset, and a hardcoded password. Code review missed it. It always does.

daggate is a single-file, zero-dependency static gate that enforces 16 production-readiness rules on every DAG file before merge:

- Reliability: explicit catchup and schedule, retries + retry_delay, dagrun_timeout, failure alerting wired
- Idempotency: static timezone-aware start_date, no dynamic now()/days_ago()
- Scheduler health: no Variable.get() or network/DB calls at parse time
- Security: hardcoded secrets and credential-bearing connection URLs flagged
- Operability: owner, tags, doc_md enforced

It never imports your code, so it runs anywhere Python 3.10+ runs — laptop, pre-commit, GitHub Actions, Jenkins. Errors fail the build; warnings are tunable (`--strict`, `--ignore`, `--select`). Output formats: text, JSON, GitHub annotations.

ruff's AIR rules handle Airflow 3 migration syntax. daggate handles production policy. Run both.

**What you get (ZIP, v1.0.0):**
- daggate.py — the linter (16 rules, single file, MIT-free of dependencies)
- 30-test pytest suite proving every rule
- Good + bad example DAGs
- 7-section Production Readiness Checklist (the human-review half)
- Drop-in GitHub Actions workflow, Jenkins stage, pre-commit hook
- FAQ, troubleshooting guide, changelog, 12 months of 1.x updates

**Compatibility:** Python 3.10–3.13. Airflow 2.x and 3.x DAG files (classic, context-manager, and @dag TaskFlow styles). Not for runtime-generated DAG factories (see FAQ).

**License:** Commercial, single team (≤25 engineers, one company). Internal modification allowed. No redistribution.

**Support:** Defect fixes for 12 months. 14-day refund, no questions.

**Disclaimer:** Independent product. Not affiliated with or endorsed by the Apache Software Foundation. "Apache Airflow" is a trademark of the ASF, used for identification only.

**Tags:** airflow, apache airflow, data engineering, DAG, linter, CI/CD, data pipeline, devops, static analysis, pre-commit

**FAQ block (Gumroad):** copy docs/FAQ.md top 5 answers.
