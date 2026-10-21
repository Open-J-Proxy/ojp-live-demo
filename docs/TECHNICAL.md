# Technical reference

## Connection architecture

The Direct JDBC path uses HikariCP with `minimumIdle=200` and
`maximumPoolSize=200`. The OJP application path has no local HikariCP pool:
each transaction opens and closes a logical JDBC connection, while OJP's
backend pool has a minimum idle and maximum of 10. HTTP capacity remains
512 workers by default. These sizes are intentionally asymmetric.

PostgreSQL sessions from the Direct pool use a per-process, unique
`ApplicationName` (`demo-direct-*`). A mode switch first excludes and drains
active database operations, closes HikariCP, waits for that exact application's
sessions, then—after a grace period—terminates only sessions matching the
unique application name, database, and configured database user. It verifies
the sessions are gone before proceeding. It never targets unrelated sessions.
Control requests have their own timeout, separate from measured SQL request
timeouts.

## HTAP workload and slow-query segregation

The workload executes ShopService-style SQL: product browse, order history,
checkout, review writes, account lookup, and aggregate report queries. The
default exact-ratio schedule is 98% OLTP and 2% OLAP. The ordered RPS profile is
`8,256,8,8`: a baseline, a sharp traffic spike, and two baseline windows to
observe how each mode behaves after offered load drops. This is an ordered
sequence of independent rate-level trials, not a single uninterrupted traffic
trace. Before every measured level, outstanding work drains, the data baseline
resets, and a separate warm-up runs. Direct JDBC and OJP execute sequentially
with the same seed, reset baseline, request plan, and RPS levels.

`scripts/htap.sh` recreates the OJP service with segregation enabled,
`slowSlotPercentage=20`, absolute-threshold classification, and a configurable
positive threshold (`SLOW_QUERY_THRESHOLD_MS`, default 100). It verifies the
running container environment and JVM startup options before generating load.
This is the only benchmark entrypoint and always measures Direct JDBC followed
by OJP. It rebuilds the load image from the current checkout before starting
the runner, preventing obsolete cached CLI options from breaking the run.
Single-path and warmup-only runner controls remain internal to regression tests;
HA fault orchestration is invoked directly by the integration suite.
The runner compares OJP's real `ojp_sql_slow_executions_total` counters around
the measurement. No observed classifications are reported as “not
demonstrated”; configuration by itself is not treated as evidence that SQL
entered the slow lane.

## Metrics and result retention

Application request and load metrics use bounded labels for `mode`, `op`,
`phase`, `scenario`, and `level`. Logical JDBC opened/closed counters carry
`mode="direct|ojp"`. `demo_postgres_physical_connections` distinguishes
application-owned Direct sessions and OJP backend sessions by mode.

At each successful measurement, the load runner publishes a saved summary
labelled by mode, scenario, phase, and RPS level: successful throughput, OLTP
and OLAP P95, error percentage, dropped requests, physical connections, and
PostgreSQL CPU as `demo_benchmark_result_*` Prometheus series (not shown on the
Grafana dashboard). This allows sequential Direct/OJP results to be compared side
by side without pretending their database gauges were sampled simultaneously.
Run IDs remain in JSON/CSV result artifacts, not Prometheus labels.

The Grafana dashboard shows offered/dispatched/completed RPS, server and
end-to-end latency, OLTP versus OLAP P95, errors/rollbacks/drops, Direct
HikariCP and OJP backend pool gauges, logical connections, OJP SQL
classifications, and PostgreSQL CPU and memory. Grafana provisioning is in
`monitoring/grafana/provisioning/`.

OJP's scrape endpoint also exports live pool gauges and actual SQL execution /
slow-execution counters. A missing slow-execution series is not proof that a
query was classified. The benchmark records the absence or increase explicitly.

## Configuration

| Variable | Default | Purpose |
|---|---:|---|
| `DIRECT_POOL_SIZE` | 200 | Direct HikariCP minimum and maximum |
| `OJP_POOL_SIZE` | 10 | OJP backend pool maximum |
| `OJP_MIN_IDLE` | 10 | OJP backend idle target |
| `HTTP_WORKERS` | 512 | Bounded application workers (minimum 200) |
| `HTTP_QUEUE_SIZE` | 1024 | Bounded HTTP queue |
| `STARTUP_TIMEOUT_SECONDS` | 180 | Application initialization and switching |
| `SQL_TIMEOUT_SECONDS` | 30 | Database statement timeout |
| `SLOW_QUERY_THRESHOLD_MS` | 100 | HTAP slow-classification threshold |
| `PG_MAX_CONNECTIONS` | 260 | PostgreSQL maximum including monitoring/admin |
| `RESULTS_DIR` | `results/` | JSON, CSV, and resource evidence |

Other documented Compose image and local port overrides are in `compose.yaml`.
All published ports bind to loopback for this local demonstration.

## Validation

```bash
python3 -m unittest discover -s load -p 'test_*.py' -v
mvn -f app/pom.xml verify
bash scripts/test-integration.sh
```

The live Java database test is gated by `LIVE_DB_TESTS=true`; ordinary Maven
verification without that environment variable skips its PostgreSQL/OJP test.
The GitHub Actions workflow runs Python tests, Java verification with live
database coverage, and the full 200-session integration profile.
