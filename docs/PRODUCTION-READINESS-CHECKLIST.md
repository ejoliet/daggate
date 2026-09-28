# Airflow DAG Production Readiness Checklist

Companion to `daggate`. Items marked **[DG###]** are enforced automatically;
the rest require human review. Use per-DAG before first production deploy and
after major changes.

## 1. Scheduling & time

- [ ] `schedule` explicit; `None` documented if manual-only **[DG107]**
- [ ] `catchup` set deliberately; backfill window understood **[DG101]**
- [ ] `start_date` static and timezone-aware **[DG203, DG204]**
- [ ] Data interval logic uses `{{ data_interval_start }}` / `{{ ds }}`, never wall-clock time
- [ ] DST behavior checked for non-UTC schedules

## 2. Failure handling

- [ ] `retries` >= 1 with sensible `retry_delay` **[DG102, DG103]**
- [ ] Failure alerting wired (callback -> Slack/SNS/PagerDuty) **[DG402]**
- [ ] `dagrun_timeout` and long-task `execution_timeout` set **[DG401]**
- [ ] Partial-failure behavior defined: which tasks are safe to retry alone?
- [ ] On-call runbook exists and is linked in `doc_md`

## 3. Idempotency & backfills

- [ ] Every task safe to re-run: overwrite partitions, no blind INSERT/append
- [ ] Outputs keyed by logical date, not execution wall-clock
- [ ] `depends_on_past` justified; `max_active_runs=1` if used **[DG403]**
- [ ] Backfill for 7 days tested in staging without duplicates

## 4. Parse-time hygiene

- [ ] No top-level API/DB/file calls **[DG201, DG202]**
- [ ] Heavy imports deferred inside task callables
- [ ] DAG file parses in < 1s (scheduler parse budget)

## 5. Secrets & config

- [ ] No literals: use Connections, Variables, or a secrets backend **[DG301, DG302]**
- [ ] Environment-specific values injected, not branched in code
- [ ] IAM/service-account permissions least-privilege

## 6. Data quality & contracts

- [ ] Row-count / freshness / schema checks between extract and load
- [ ] Upstream dataset SLAs known; sensors have timeouts and `mode="reschedule"`
- [ ] Downstream consumers identified; breaking-change process agreed

## 7. Operability

- [ ] `owner`, `tags`, `doc_md` filled — findable and explained **[DG104-DG106]**
- [ ] Logs actionable: task logs state inputs, outputs, and row counts
- [ ] Resource sizing checked (pool/queue/executor limits)
- [ ] Cost of the schedule estimated (warehouse credits, egress)

## Sign-off

| Role | Name | Date |
|------|------|------|
| Author | | |
| Reviewer | | |
| On-call rep | | |
