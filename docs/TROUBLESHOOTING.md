# Troubleshooting

**`exit=2` with "syntax error"** — daggate found a Python file it cannot
parse. Fix the syntax or exclude the file (point daggate at your dags folder
only, not vendored code).

**Rule fires on a non-DAG file** — daggate checks every `.py` under the path.
Keep utility modules out of the scanned path, or `--ignore` secret rules for
config modules.

**DG102 fires but tasks set retries individually** — policy decision: daggate
requires `retries` in `default_args` so no new task can silently ship with 0.
If you disagree, `--ignore DG102`.

**DG202 fires on a module-level constant** — anything calling into
requests/boto3/pandas/DB drivers at import time is flagged, even if cheap.
Move it inside a task callable or a function.

**GitHub annotations not appearing** — ensure the step runs with
`--format github` and the job doesn't swallow stdout. Non-zero exit is
expected on findings; use `continue-on-error: true` if you want soft-fail.

**Windows paths** — use forward slashes or quote the path; daggate uses
pathlib and handles both.
