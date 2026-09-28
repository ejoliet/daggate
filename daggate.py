#!/usr/bin/env python3
"""daggate — production readiness gate for Apache Airflow DAGs.

Single-file, stdlib-only, AST-based static checks. Does NOT import Airflow,
so it runs anywhere: laptops, CI runners, pre-commit hooks.

Usage:
    python daggate.py dags/
    python daggate.py dags/ --format json
    python daggate.py dags/ --format github --strict
    python daggate.py dags/ --select DG101,DG301 --ignore DG105

Exit codes: 0 = clean (or warnings only), 1 = errors found (or warnings
with --strict), 2 = usage/parse failure.

AIDEV-NOTE: complements ruff AIR rules (Airflow 3 migration). daggate covers
production POLICY: retries, catchup, secrets, top-level code, alerting.

Copyright (c) 2026. Commercial license — see LICENSE.md. Do not redistribute.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path

__version__ = "1.0.0"

ERROR = "error"
WARN = "warn"

RULES: dict[str, tuple[str, str]] = {
    # code: (severity, summary)
    "DG101": (ERROR, "DAG must set `catchup` explicitly"),
    "DG102": (ERROR, "No `retries` configured in default_args"),
    "DG103": (WARN, "`retries` set without `retry_delay`"),
    "DG104": (WARN, "Missing `owner` in default_args"),
    "DG105": (WARN, "DAG has no `tags`"),
    "DG106": (WARN, "DAG has no `doc_md` or `description`"),
    "DG107": (ERROR, "DAG must set `schedule` explicitly (use None for manual-only)"),
    "DG201": (ERROR, "Variable.get() at module top level (runs on every parse)"),
    "DG202": (ERROR, "Expensive call at module top level (network/DB/file I/O)"),
    "DG203": (ERROR, "Dynamic start_date (now()/days_ago()) breaks idempotency"),
    "DG204": (WARN, "Naive datetime start_date (no timezone)"),
    "DG301": (ERROR, "Hardcoded secret in assignment"),
    "DG302": (ERROR, "Credentials embedded in URL literal"),
    "DG401": (WARN, "DAG has no `dagrun_timeout`"),
    "DG402": (WARN, "No failure alerting (on_failure_callback / email_on_failure)"),
    "DG403": (WARN, "depends_on_past=True without max_active_runs=1"),
}

SECRET_NAME_RE = re.compile(
    r"(password|passwd|secret|api_key|apikey|access_key|auth_token|token)$", re.I
)
# scheme://user:pass@host — credentials inline in a connection string
CRED_URL_RE = re.compile(r"^[a-z][a-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@", re.I)
# obvious non-secrets to skip (env var indirection, templates, placeholders)
PLACEHOLDER_RE = re.compile(r"^(\{\{.*\}\}|\$\{?[A-Z_]+\}?|<[^>]+>|change_?me|xxx+|\*+|)$", re.I)

EXPENSIVE_TOP_LEVEL = {
    # attribute-call roots or dotted prefixes considered I/O at import time
    "requests", "boto3", "urllib", "httpx", "psycopg2", "pymysql",
    "snowflake", "sqlalchemy", "redis", "pymongo",
}
EXPENSIVE_PANDAS_FUNCS = {"read_csv", "read_parquet", "read_sql", "read_json", "read_excel"}
DYNAMIC_DATE_CALLS = {"now", "today", "utcnow", "days_ago"}


@dataclass
class Finding:
    file: str
    line: int
    code: str
    severity: str
    message: str


def _call_name(node: ast.Call) -> str:
    """Return dotted name of a call: 'Variable.get', 'pendulum.now', 'DAG'."""
    parts: list[str] = []
    f = node.func
    while isinstance(f, ast.Attribute):
        parts.append(f.attr)
        f = f.value
    if isinstance(f, ast.Name):
        parts.append(f.id)
    return ".".join(reversed(parts))


def _kw(call: ast.Call, name: str) -> ast.expr | None:
    for k in call.keywords:
        if k.arg == name:
            return k.value
    return None


def _is_dag_call(node: ast.Call) -> bool:
    name = _call_name(node)
    return name == "DAG" or name.endswith(".DAG")


def _dict_get(d: ast.expr | None, key: str) -> ast.expr | None:
    if not isinstance(d, ast.Dict):
        return None
    for k, v in zip(d.keys, d.values):
        if isinstance(k, ast.Constant) and k.value == key:
            return v
    return None


class DagFileChecker(ast.NodeVisitor):
    def __init__(self, path: str, source: str):
        self.path = path
        self.findings: list[Finding] = []
        self.tree = ast.parse(source, filename=path)
        # map: variable name -> dict node, for default_args defined separately
        self.dict_assignments: dict[str, ast.Dict] = {}
        self._collect_dict_assignments()

    def _collect_dict_assignments(self) -> None:
        for node in self.tree.body:
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict):
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name):
                        self.dict_assignments[tgt.id] = node.value

    def emit(self, node: ast.AST, code: str) -> None:
        sev, msg = RULES[code]
        self.findings.append(
            Finding(self.path, getattr(node, "lineno", 0), code, sev, msg)
        )

    # ---------- top-level (module scope) checks ----------

    def check_top_level(self) -> None:
        for node in self.tree.body:
            for call in _walk_calls_shallow(node):
                name = _call_name(call)
                root = name.split(".")[0]
                if name.endswith("Variable.get") or name == "Variable.get":
                    self.emit(call, "DG201")
                elif root in EXPENSIVE_TOP_LEVEL:
                    self.emit(call, "DG202")
                elif name.split(".")[-1] in EXPENSIVE_PANDAS_FUNCS and root in ("pd", "pandas"):
                    self.emit(call, "DG202")

    # ---------- secrets ----------

    def check_secrets(self) -> None:
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Assign):
                for tgt in node.targets:
                    if (
                        isinstance(tgt, ast.Name)
                        and SECRET_NAME_RE.search(tgt.id)
                        and isinstance(node.value, ast.Constant)
                        and isinstance(node.value.value, str)
                        and node.value.value
                        and not PLACEHOLDER_RE.match(node.value.value)
                    ):
                        self.emit(node, "DG301")
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if CRED_URL_RE.match(node.value):
                    self.emit(node, "DG302")

    # ---------- DAG definition checks ----------

    def check_dags(self) -> None:
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call) and _is_dag_call(node):
                self._check_dag_call(node)
            # @dag decorator (TaskFlow API)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for dec in node.decorator_list:
                    if isinstance(dec, ast.Call) and _call_name(dec) in ("dag", "airflow.dag"):
                        self._check_dag_call(dec)

    def _resolve_default_args(self, call: ast.Call) -> ast.Dict | None:
        val = _kw(call, "default_args")
        if isinstance(val, ast.Dict):
            return val
        if isinstance(val, ast.Name):
            return self.dict_assignments.get(val.id)
        return None

    def _check_dag_call(self, call: ast.Call) -> None:
        da = self._resolve_default_args(call)

        if _kw(call, "catchup") is None:
            self.emit(call, "DG101")

        retries = _dict_get(da, "retries")
        if retries is None:
            # AIDEV-NOTE: task-level retries can't be reliably attributed to a
            # DAG statically; policy = require default_args retries.
            self.emit(call, "DG102")
        elif (
            isinstance(retries, ast.Constant)
            and isinstance(retries.value, int)
            and retries.value > 0
            and _dict_get(da, "retry_delay") is None
        ):
            self.emit(call, "DG103")

        if _dict_get(da, "owner") is None:
            self.emit(call, "DG104")

        if _kw(call, "tags") is None:
            self.emit(call, "DG105")

        if _kw(call, "doc_md") is None and _kw(call, "description") is None:
            self.emit(call, "DG106")

        if _kw(call, "schedule") is None and _kw(call, "schedule_interval") is None:
            self.emit(call, "DG107")

        self._check_start_date(call, da)

        if _kw(call, "dagrun_timeout") is None:
            self.emit(call, "DG401")

        has_alert = (
            _kw(call, "on_failure_callback") is not None
            or _dict_get(da, "on_failure_callback") is not None
            or _truthy(_dict_get(da, "email_on_failure"))
            or _kw(call, "sla_miss_callback") is not None
        )
        if not has_alert:
            self.emit(call, "DG402")

        dop = _dict_get(da, "depends_on_past")
        if _truthy(dop):
            mar = _kw(call, "max_active_runs")
            if not (isinstance(mar, ast.Constant) and mar.value == 1):
                self.emit(call, "DG403")

    def _check_start_date(self, call: ast.Call, da: ast.Dict | None) -> None:
        sd = _kw(call, "start_date") or _dict_get(da, "start_date")
        if sd is None:
            return
        for sub in ast.walk(sd):
            if isinstance(sub, ast.Call):
                name = _call_name(sub)
                leaf = name.split(".")[-1]
                if leaf in DYNAMIC_DATE_CALLS:
                    self.emit(sub, "DG203")
                    return
                if leaf == "datetime" and not name.startswith("pendulum"):
                    # naive unless tzinfo/tz kwarg present
                    if _kw(sub, "tzinfo") is None and _kw(sub, "tz") is None:
                        self.emit(sub, "DG204")

    def run(self) -> list[Finding]:
        self.check_top_level()
        self.check_secrets()
        self.check_dags()
        return self.findings


def _truthy(node: ast.expr | None) -> bool:
    return isinstance(node, ast.Constant) and bool(node.value) is True


def _walk_calls_shallow(node: ast.stmt):
    """Yield calls in module-scope statements, without descending into
    function/class/lambda bodies (code there runs at task time, not parse time)."""

    def _recurse(n: ast.AST):
        for child in ast.iter_child_nodes(n):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
                continue
            if isinstance(child, ast.Call):
                yield child
            yield from _recurse(child)

    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return
    if isinstance(node, ast.Call):
        yield node
    yield from _recurse(node)


def check_path(path: Path) -> tuple[list[Finding], list[str]]:
    findings: list[Finding] = []
    parse_errors: list[str] = []
    files = [path] if path.is_file() else sorted(path.rglob("*.py"))
    for f in files:
        try:
            src = f.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as e:
            parse_errors.append(f"{f}: unreadable ({e})")
            continue
        try:
            checker = DagFileChecker(str(f), src)
        except SyntaxError as e:
            parse_errors.append(f"{f}:{e.lineno}: syntax error: {e.msg}")
            continue
        findings.extend(checker.run())
    return findings, parse_errors


def format_text(findings: list[Finding]) -> str:
    lines = [
        f"{fi.file}:{fi.line}: {fi.code} [{fi.severity}] {fi.message}"
        for fi in findings
    ]
    errs = sum(1 for f in findings if f.severity == ERROR)
    warns = len(findings) - errs
    lines.append(f"\ndaggate: {errs} error(s), {warns} warning(s)")
    return "\n".join(lines)


def format_github(findings: list[Finding]) -> str:
    out = []
    for fi in findings:
        level = "error" if fi.severity == ERROR else "warning"
        out.append(
            f"::{level} file={fi.file},line={fi.line},title={fi.code}::{fi.message}"
        )
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="daggate", description=__doc__.splitlines()[0])
    ap.add_argument("path", help="DAG file or directory")
    ap.add_argument("--format", choices=["text", "json", "github"], default="text")
    ap.add_argument("--select", default="", help="comma-separated rule codes to run")
    ap.add_argument("--ignore", default="", help="comma-separated rule codes to skip")
    ap.add_argument("--strict", action="store_true", help="warnings also fail (exit 1)")
    ap.add_argument("--version", action="version", version=f"daggate {__version__}")
    args = ap.parse_args(argv)

    p = Path(args.path)
    if not p.exists():
        print(f"daggate: path not found: {p}", file=sys.stderr)
        return 2

    findings, parse_errors = check_path(p)

    select = {c.strip().upper() for c in args.select.split(",") if c.strip()}
    ignore = {c.strip().upper() for c in args.ignore.split(",") if c.strip()}
    if select:
        findings = [f for f in findings if f.code in select]
    if ignore:
        findings = [f for f in findings if f.code not in ignore]

    for e in parse_errors:
        print(f"daggate: {e}", file=sys.stderr)

    if args.format == "json":
        print(json.dumps([asdict(f) for f in findings], indent=2))
    elif args.format == "github":
        print(format_github(findings))
    else:
        print(format_text(findings))

    if parse_errors:
        return 2
    has_error = any(f.severity == ERROR for f in findings)
    has_warn = any(f.severity == WARN for f in findings)
    if has_error or (args.strict and has_warn):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
