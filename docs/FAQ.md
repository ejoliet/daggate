# FAQ

**Does daggate need Airflow installed?** No. It parses your Python files with
the standard library `ast` module and never imports or executes them.

**Which Airflow versions are supported?** DAG files written for Airflow 2.x
and 3.x. daggate accepts both `schedule` and legacy `schedule_interval`.

**How is this different from ruff's AIR rules?** ruff AIR rules detect
Airflow 3 migration issues (removed imports/params). daggate enforces
production policy: retries, catchup, secrets, parse-time hygiene, alerting,
timeouts. Run both.

**A rule doesn't fit our policy.** Disable it: `--ignore DG105,DG401`.
Or run only a subset: `--select DG301,DG302`.

**We generate DAGs dynamically (dag-factory, loops over YAML).** Static
analysis can't see through runtime generation. Run daggate on your template
DAG or generated output, and use the checklist for the rest.

**False positive on a secret?** DG301 skips template placeholders like
`{{ var.value.x }}`, `${ENV_VAR}`, and `<placeholder>`. If you hit another
pattern, `--ignore DG301` for that run and report it as a defect.

**Can I modify the code?** Yes, internally — see LICENSE.md. Redistribution
is not permitted.

**Refunds?** 14 days, no questions, via the purchase platform.

**Support scope.** Defect reports for 12 months. Custom rules, consulting,
and integration help are out of scope (enterprise license available).
