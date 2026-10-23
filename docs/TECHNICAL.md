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

### Java source-file producers

`load/Runner.java` replaces the Python workload producer. It runs on Java 25
LTS as a compact source file with an instance `main` method: JEP 445 introduced
this style as a preview, and [JEP 512](https://openjdk.org/jeps/512) finalized it
in Java 25. No `--enable-preview`, separate compilation, Maven build, or external
dependencies are required. The load image includes a JDK, not just a JRE,
because the Java source launcher compiles the scripts at startup.

The producer retains the existing CLI, JSON/CSV result schema, fixed-rate
open-loop arrivals, exact-ratio workload blocks, bounded outstanding requests,
and separate request/control timeouts. Requests use virtual threads; reaching
`--max-in-flight` records a drop rather than blocking the arrival schedule.
Warm-up and measurement drain independently, and Direct/OJP use the same seeded
work plan. Java's seeded random sequence need not match the former Python
sequence, so historical results should not be assumed to use identical requests.

Docker remains the normal execution path via `scripts/htap.sh`. For development
with a local JDK 25, inspect the producer CLI directly:

```bash
java load/Runner.java --help
```

The load container's internal HTTP bridge and controlled-delay integration
proxy are also Java source-file scripts. Host-side control, resource sampling,
failover orchestration, and integration/regression harnesses remain Python 3;
the application itself still targets Java 21.

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

### Grafana metrics reference

Prometheus scrapes every 2 seconds (`monitoring/prometheus.yml`) from three
places: the demo app (`app:8081/metrics`), the OJP server (`ojp:9159`), and
postgres-exporter (`postgres-exporter:9187`). The app also re-publishes values
it does not measure itself: load-generator counts pushed to `/arrivals` by
`load/Runner.java`, and PostgreSQL container CPU/memory pushed to `/resources` by
`scripts/resources.py`.

| Panel | Metric | What it means | How and where it is collected |
|---|---|---|---|
| Offered / dispatched / SQL-completed RPS | `demo_load_scheduled_total` | Requests the load generator planned to send | Counted by `load/Runner.java` on a fixed timetable; pushed to the app's `/arrivals` about once a second |
| | `demo_load_dispatched_total` | Requests actually sent | Counted by `load/Runner.java`; pushed to `/arrivals` |
| | `demo_completions_total` (excluding `hold`/`rollback`) | Transactions that committed | +1 in the app (`Database.work`) after each commit |
| Server P95 / P99 latency | `demo_request_duration_seconds` | Time spent inside the app per request: getting a connection plus running the SQL | Timer in the app from the start of the transaction to commit or rollback; queueing before the app picks up the request is not included |
| SQL success / errors / rollback rates | `demo_completions_total` | Committed transactions | +1 in the app per commit |
| | `demo_errors_total` | Failed transactions | +1 in the app when a transaction throws |
| | `demo_rollbacks_total` | Rolled-back transactions | +1 in the app whenever it rolls a transaction back |
| Direct HikariCP physical pool | `demo_hikari_active` / `_idle` / `_total` | Connections in the app's own HikariCP pool (Direct mode); 0 in OJP mode | Read from HikariCP's pool counters each time Prometheus scrapes the app |
| OJP backend physical pool | `ojp_hikari_pool_connections_active_connections` / `_idle_connections` / `_max_connections` | OJP's pool of real PostgreSQL connections | Exported by the OJP server on port 9159 |
| P95 OLTP versus OLAP | `demo_request_duration_seconds` | Server latency split into analytical `report` queries (OLAP) and all other transactions (OLTP) | Same app timer as Server P95, filtered by the `op` label |
| HTTP workers / admission queue / requests in flight | `demo_http_workers`, `demo_http_workers_active`, `demo_http_queue`, `demo_inflight` | Worker capacity, busy workers, queued requests, and requests being processed | Read from the app's HTTP worker pool on each scrape |
| | `demo_rejected_total` | Requests turned away because all worker slots were taken | +1 in the app when a request cannot be admitted |
| P95 connection acquisition | `demo_acquisition_duration_seconds` | Time the app waits for a connection | App timer around getting a connection: from HikariCP in Direct mode, or a new logical connection to OJP in OJP mode |
| OJP backend pending threads | `ojp_hikari_pool_connections_pending_threads` | Requests inside OJP waiting for a free backend connection | Exported by the OJP server |
| Current mode / scenario / phase / RPS level | `demo_context` | The labels the load runner last set on the app | Set through the app's admin endpoint; exported on each scrape |
| OJP logical connections opened / closed | `demo_logical_connections_opened_total` / `_closed_total` (by `mode`) | JDBC connections the app opened and closed | +1 in the app on every connection open and close |
| Actual OJP SQL classifications | `ojp_sql_executions_total`, `ojp_sql_slow_executions_total` | All SQL that OJP ran, and the subset it classified as slow | Exported by the OJP server; the slow series appears only after a query is actually classified as slow |
| Scheduled-arrival to completion P95 / P99 | `demo_load_latency_seconds` | What a user would experience: time from the planned send time to the reply, including client and server queueing | Measured by `load/Runner.java`; histogram pushed to `/arrivals` |
| Generator drops / timeouts / requests in flight | `demo_load_dropped_total` | Planned requests the generator skipped because too many requests were already in flight (`max_in_flight`) | Counted by `load/Runner.java`; pushed to `/arrivals` |
| | `demo_load_timeouts_total` | Sent requests that got no reply before the client timeout | Counted by `load/Runner.java`; pushed to `/arrivals` |
| | `demo_load_inflight` | Requests sent and still waiting for a reply | Counted by `load/Runner.java`; pushed to `/arrivals` |
| PostgreSQL container CPU | `demo_postgres_cpu_percent` | PostgreSQL container CPU; 100% means one core | `scripts/resources.py` runs `docker stats` every second and posts to the app's `/resources`. The panel shows a value only if a sample arrived in the last 10 seconds |
| PostgreSQL container memory | `demo_postgres_memory_percent` | PostgreSQL container memory as a percentage of its limit | Same `docker stats` sampler as CPU |

Latency appears in four panels because each one times a different part of a
request. A large gap between end-to-end and server latency means time is lost
to queueing rather than SQL. A large share of server latency spent in
connection acquisition indicates pool pressure.

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
# JDK 25 must be available on PATH for producer regression tests.
python3 -m unittest discover -s load -p 'test_*.py' -v
mvn -f app/pom.xml verify
bash scripts/test-integration.sh
```

The live Java database test is gated by `LIVE_DB_TESTS=true`; ordinary Maven
verification without that environment variable skips its PostgreSQL/OJP test.
The GitHub Actions workflow installs Temurin 25 for the Java source-file
producer regressions driven by Python unittest, runs application verification
on Java 21 with live database coverage, and runs the full 200-session
integration profile with the Java 25 load image.
