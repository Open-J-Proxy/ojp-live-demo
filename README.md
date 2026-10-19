# Open J Proxy live benchmark

A reproducible, local-only JDBC experiment: **direct HikariCP (200 physical
connections)** versus **unpooled OJP logical connections (40 backend connections)**.
Neither configuration is assumed to win. These are different capacity-management
architectures, **not an equal-pool-size microbenchmark**.

## Requirements and quick start

Use Linux with Docker Engine, **Docker Compose v2**, Bash, Python 3.10+, curl, and
at least 4 CPU cores / 12 GB available RAM / 10 GB free disk. More headroom is
recommended for 200 PostgreSQL processes, 512 HTTP workers, and three OJP nodes.
The load generator shares the host in this portable setup; use a dedicated load
host for publishable performance claims. Host Java/Maven are optional; builds use
Java 21 inside Docker.

On Ubuntu, install Docker's official apt repository following
https://docs.docker.com/engine/install/ubuntu/, then:

```bash
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
# Log out/in before using Docker without sudo.
docker version
docker compose version
```

From the repository root:

```bash
bash scripts/verify-environment.sh
bash scripts/start.sh
bash scripts/verify-connections.sh
bash scripts/death-spiral.sh --mode both --rps 8,16,32,64,128,256 \
  --warmup 30 --duration 60 --max-in-flight 512 \
  --oltp-percent 90 --olap-percent 10 --request-timeout 30 \
  --seed 12345 --run-id conference-01
bash scripts/stop.sh
```

Scripts resolve paths relative to their own location, not the caller's working
directory. `--help` documents individual entrypoints. First startup builds the
application and runner, seeds data once, checks service health and probes real SQL.
Subsequent startup retains the named PostgreSQL volume.

Grafana: http://localhost:3000/d/ojp-benchmark

Prometheus: http://localhost:9090

Application readiness: http://localhost:8080/ready

Grafana's local-demo login is **admin / demo-local-only**. To override settings,
create an ignored `.env` at the repository root:

```dotenv
GRAFANA_USER=admin
GRAFANA_PASSWORD=choose-a-local-demo-password
DB_PASSWORD=choose-a-local-database-password
APP_PORT=8080
GRAFANA_PORT=3000
PROMETHEUS_PORT=9090
POSTGRES_PORT=5432
```

Changing database or Grafana passwords does not rewrite credentials in existing
volumes. Change those credentials through their respective administration tools
or explicitly regenerate the demo volumes.

**Security boundary:** all published ports bind to `127.0.0.1`. Database
credentials and unauthenticated benchmark controls are suitable only for an
isolated demonstration. Do not expose these services to untrusted networks.
The application also has an internal control/metrics listener on 8081 so
diagnostics remain available while workload workers are busy; it is not published.
Destructive controls must never be used during another person's benchmark.
Optionally set `ADMIN_TOKEN` to protect control POSTs with a bearer token;
Compose forwards it to the application and load generator. This does not
authenticate workload GETs or replace the local-only network boundary.

## Architecture and verified upstream contracts

```text
Python open-loop scheduler -> Java 21 HTTP service (512 workers)
  direct: HikariCP min=max200 -> PostgreSQL 16
  ojp: DriverManager.getConnection()/close per transaction
       -> OJP JDBC Type 3 driver -> gRPC/HTTP2 -> OJP -> backend pool max40/min10

Prometheus <- application, OJP, PostgreSQL exporter
Grafana -> provisioned Prometheus datasource and benchmark dashboard
```

Pinned OJP release: **1.0.3**, image **`rrobetti/ojp:1.0.3`**, JDBC artifact
**`org.openjproxy:ojp-jdbc-driver:1.0.3`**, driver
`org.openjproxy.jdbc.Driver`. The OJP image bundles PostgreSQL JDBC and uses its
own Java runtime (the demo application is Java 21).

Verified against the release source, not inferred from old examples:

* [Driver and properties transport](https://github.com/Open-J-Proxy/ojp/blob/v1.0.3/ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Driver.java)
* [Pool configuration](https://github.com/Open-J-Proxy/ojp/blob/v1.0.3/documents/connection-pool/configuration.md)
* [Server configuration](https://github.com/Open-J-Proxy/ojp/blob/v1.0.3/documents/configuration/ojp-server-configuration.md)
* [Multi-node behavior](https://github.com/Open-J-Proxy/ojp/blob/v1.0.3/documents/multinode/README.md)
* [Original ShopService entities](https://github.com/Open-J-Proxy/ojp-framework-integration/tree/main/spring-boot/shopservice/src/main/java/com/example/shopservice/entity)

Single-node URL:

```text
jdbc:ojp[ojp:1059]_postgresql://postgres:5432/shop?ApplicationName=demo-ojp
```

Multi-node URL:

```text
jdbc:ojp[ojp:1059,ojp2:1059,ojp3:1059]_postgresql://postgres:5432/shop?ApplicationName=demo-ojp
```

Pool properties passed in JDBC `Properties` are transported to the server:

```properties
ojp.connection.pool.maximumPoolSize=40
ojp.connection.pool.minimumIdle=10
ojp.connection.pool.connectionTimeout=30000
```

There is **no client HikariDataSource in OJP mode**. Every measured operation
opens a logical connection with `DriverManager`, executes and commits its
transaction, and closes it with try-with-resources. OJP's backend pool stays
enabled. **Never set `ojp.connection.pool.enabled=false` to remove local pooling.**

Direct mode sets both `minimumIdle` and `maximumPoolSize` to 200, proactively
borrows all connections during initialization, and polls **both Hikari and
`pg_stat_activity`** before reporting readiness. PostgreSQL `application_name`
is `demo-direct`; OJP sessions use `demo-ojp`; control checks use `demo-admin`.
Missing/unavailable session telemetry is not interpreted as zero.

Mode switching excludes workload transactions, drains active operations,
closes the old Hikari pool, and waits for all `demo-direct` sessions to vanish.
The runner executes `both` **sequentially**. Do not launch concurrent benchmark
runners against the same application.

### Configuration

| Environment variable | Default | Purpose |
|---|---:|---|
| `PG_MAX_CONNECTIONS` | 260 | PostgreSQL limit, including monitoring/admin reserve |
| `DIRECT_POOL_SIZE` | 200 | Direct minimum and maximum; never silently reduced |
| `OJP_POOL_SIZE` | 40 | OJP backend maximum |
| `OJP_MIN_IDLE` | 10 | OJP backend idle target |
| `HTTP_WORKERS` | 512 | Bounded HTTP worker capacity, minimum 200 |
| `HTTP_QUEUE_SIZE` | 1024 | Bounded HTTP admission queue |
| `STARTUP_TIMEOUT_SECONDS` | 180 | Pool establishment / mode-switch deadline |
| `TEST_ENDPOINTS` | false | Controlled no-DB concurrency and forced rollback probes |
| `SLOW_QUERY_ENABLED` | false | OJP slow-query segregation |
| `SLOW_QUERY_THRESHOLD_MS` | 100 | Absolute slow-classification threshold |
| `DB_PASSWORD` | demo-local-only | Compose database/app/exporter credential |
| `OJP_URL` | single-node URL above | Driver endpoint list / backend URL |
| `RESULTS_DIR` | repository `results/` | Host results directory |

Application-specific overrides use `PG_URL`, `PG_USER`, `PG_PASSWORD`,
`SQL_TIMEOUT_SECONDS` (30 by default), and `MODE=direct|ojp` inside the container.
Compose supplies these from its service configuration. Image versions can be
overridden using `POSTGRES_IMAGE`, `OJP_IMAGE`, `MAVEN_IMAGE`, `JAVA_IMAGE`,
`PROMETHEUS_IMAGE`, `GRAFANA_IMAGE`, and `POSTGRES_EXPORTER_IMAGE`.
Compose resource caps are visible in `compose.yaml`; keep them identical across
comparisons. The HTTP executor's active workers, queue, rejections and in-flight
requests are observable. Excess arrivals are not allowed to grow an unbounded
application or load-generator queue.
After changing OJP backend pool settings, recreate OJP as well as the application:
`docker compose up -d --force-recreate ojp app`. Server-owned datasources may
otherwise retain their original pool configuration. Verify the actual OJP
maximum/minimum gauges, not just the application's requested settings.

If your network intercepts Maven Central TLS with an enterprise CA, supply the
trusted **public CA certificate** as a BuildKit secret:

```bash
MAVEN_EXTRA_CA=/absolute/path/to/trusted-ca.pem bash scripts/start.sh
```

It is imported only into the Maven build stage's Java trust store and is not
copied into the runtime image. Do not disable certificate verification or commit
private keys/certificates into this repository. Ordinary builds need no override.

## ShopService data and state equivalence

The catalog domain (`users`, `products`, `reviews`) and checkout domain
(`orders`, `order_items`) are deliberately mapped into **one `shop` database**.
Column names, bigint identity IDs, decimal prices, timestamps and original
relationships are preserved. Scalar cross-domain user/product IDs gain foreign
keys because the domains share a database here. Additional constraints validate
ratings, quantities and positive prices.

| Table | Full demo rows |
|---|---:|
| users | 20,000 |
| products | 5,000 |
| orders | 200,000 |
| order_items | 600,000 |
| reviews | 80,000 |

`db/seed.sql` uses PostgreSQL bulk `INSERT ... SELECT generate_series`, a
configurable `setseed`, fixed calendar bounds, skewed customer/product popularity,
varied prices/quantities and one-to-five-item orders. The default dataset spans
24 months. Seeded reviews reference products purchased by their authors.
Indexes cover order history/date, item order/product joins, review user/product
lookup and product prices. Seeding checks all counts and runs `ANALYZE`.

```bash
bash scripts/seed-data.sh --help
bash scripts/seed-data.sh
```

Seed settings are `SEED_USERS`, `SEED_PRODUCTS`, `SEED_ORDERS`, `SEED_ITEMS`,
`SEED_REVIEWS`, `SEED` (12345). Existing data with different seed parameters is
rejected rather than silently overwritten. With the standard item count
`3 * orders`, each order has one to five items; custom inconsistent counts can
change that distribution.

Measured checkout/review writes append rows only; they do not change seeded
products, users or reviews. `benchmark_reset()` deletes appended writes and
restores identity sequences. The runner drains all warm-up work, restores the
baseline, and resets before **every mode and every RPS level**. This is a series
of independently identified intervals, not a stateful write staircase.
Warm caches are intentionally retained; resetting rows does not reset PostgreSQL
buffers, OS caches, JIT or vacuum history. Use repeated/reversed comparisons and
report this limitation.

```bash
# Stop without deleting data:
bash scripts/stop.sh
# Remove this project's persistent data ONLY after explicit confirmation:
bash scripts/reset.sh --destroy-data
# Regenerate on next start:
bash scripts/start.sh
```

## Workload and arrival methodology

| Operation | Share | Work |
|---|---:|---|
| browse | 30% | Product lookup/catalog and rating information |
| history | 25% | Customer orders/items/products joins |
| checkout | 20% | Insert order plus items in one transaction |
| review | 10% | Insert validated product review |
| account | 5% | User account lookup |
| report | 10% | Sales/date/product/customer aggregation |

All operations use prepared statements and close JDBC resources. Checkout
commits only after all items are inserted; exceptions cause rollback. The
controlled rollback probe deliberately fails a transaction; it is not part of
the measured workload. Health checks may use `SELECT 1`; measured operations do
not use sleeps or synthetic connection-holding SQL.

The Python scheduler uses a **monotonic fixed-rate open loop**:
arrival `i` is due at `start + i / target_rps`, independent of response time.
It submits at most `max-in-flight` requests. If capacity is exhausted it records
a dropped arrival and continues the original schedule instead of slowing down.
Seeded shuffled exact-ratio blocks choose operations; short partial blocks and
errors may make completed proportions differ from offered proportions.

Scheduled-to-completion latency includes scheduler delay, client dispatch,
HTTP/application queueing, connection acquisition and SQL execution. Server-side
histograms independently measure request/acquisition/SQL time. The generator
records scheduled, dispatched, HTTP-accepted, SQL-completed, successful/failed,
timed-out, dropped and in-flight requests. **202 is never SQL completion.**
Socket timeouts are not proof that a transaction did not commit; such ambiguous
outcomes count as client failures and are not retried automatically.

Each interval verifies readiness and actual SQL, establishes the selected pool,
warms representative operations, drains, restores the baseline, captures
readiness/connection evidence, measures, drains again, then exports. Warm-up
and measurement have separate `phase` labels and separate result objects.
Warm-up samples are never merged into measured percentiles. Use at least 30–60
seconds of warm-up for a presentation; a short CI warm-up validates correctness,
not JIT stability.

### Runner parameters

All scenario wrappers forward these options to the same runner:

| Option | Default |
|---|---|
| `--mode direct\|ojp\|both` | both |
| `--rps` | 8,16,32,64,128,256, in exactly that order |
| `--warmup` | 30 seconds |
| `--duration` | 60 seconds per level, plus drain |
| `--max-in-flight` | 512 |
| `--oltp-percent` / `--olap-percent` | 90 / 10; must total 100 |
| `--operation-mix` | Optional six-operation percentage CSV; otherwise the default OLTP ratios |
| `--request-timeout` | 30 seconds |
| `--seed` | 12345 |
| `--run-id` | generated unique identifier |
| `--url` | http://app:8080 in Compose |
| `--output` | mounted results directory |

OLTP percentage changes rescale the five OLTP shares in the ratio 30:25:20:10:5.
For independently configured operation shares, pass e.g.
`--operation-mix browse=30,history=25,checkout=20,review=10,account=5,report=10`.
All six keys are required; nonnegative integer values must total 100 and agree
with the requested OLTP/OLAP totals. Zero-weight operations are excluded.
Invalid modes/rates/percentages/timeouts/concurrency fail before scheduling.
Rate lists are ordered input, not hardcoded shell loops.
`bash scripts/warmup.sh --mode both --rps 8 --warmup 30` uses
`--warmup-only`: it drains and exports warm-up results without starting a
measurement interval.

## Demonstration scenarios

```bash
# A: Database death spiral, independent reset/warm-up at each level
bash scripts/death-spiral.sh --mode both --rps 8,16,32,64,128,256 --warmup 30 --duration 60

# B: Fast/analytical interference and verified 20% slow admission lane
bash scripts/slow-queries.sh --mode both --rps 16,32,64 --warmup 60 --duration 120

# C: Three real OJP nodes; stop one during traffic
bash scripts/failover.sh --mode ojp --rps 8 --warmup 10 --duration 60

# D: Offer more work than the database can serve
bash scripts/throttling.sh --mode both --rps 128,256,512 --warmup 30 --duration 60

# Sequential comparison using the same runner
bash scripts/compare.sh --rps 16,32,64 --seed 12345 --run-id comparison-01
```

Slow segregation uses verified JVM properties
`ojp.server.slowQuerySegregation.enabled=true`,
`slowSlotPercentage=20`, `classificationMode=ABSOLUTE_THRESHOLD`, and
`slowQueryThresholdMs=100` (all after the common
`ojp.server.slowQuerySegregation.` prefix). This partitions admission slots,
not an independent connection pool. Classification is learned from actual
execution time; an operation called `report` is **not automatically slow**.
Inspect real SQL slow-execution and lane metrics; if no query exceeds the
threshold, report that isolation was not demonstrated. Tune dataset size or the
threshold explicitly; do not add artificial sleeps.
Inspect `sum by (sql_statement) (ojp_sql_slow_executions_total)` in Prometheus
to verify which actual statements were classified; the counter need not exist
before the first slow execution.

HA uses the driver's comma-separated endpoint syntax. Bound transactions may
fail when their node dies; OJP does not transparently migrate or safely replay
an in-progress transaction. New connections can select surviving nodes. Record
failure time, transient errors, subsequent successes and recovery latency.
OJP distributes max/min across live nodes using ceiling division, so 40 across
three nodes may mean 14 per node (42 total), not a strict cluster-wide cap of 40.
The failover script restores the stopped node after the experiment.

Throttling is an **experiment**, not a guaranteed rejection count: a fast database
may absorb the selected rates. Compare effective throughput, client drops,
timeouts, queue depth, active/pending pool connections and actual OJP admission
metrics. Backend pending threads are not synonymous with rejected RPCs.

## Monitoring and interpreting results

Grafana provisioning installs the Prometheus datasource and dashboard without
manual import. Metrics include:

* `demo_completions_total`, `demo_errors_total`, `demo_rollbacks_total`
* `demo_load_scheduled_total`, `demo_load_dispatched_total`, `demo_load_dropped_total`,
  `demo_load_timeouts_total`, `demo_load_inflight` and the scheduled-to-completion
  histogram `demo_load_latency_seconds_bucket`
* `demo_request_duration_seconds_bucket`, `demo_acquisition_duration_seconds_bucket`,
  `demo_sql_duration_seconds_bucket`
* `demo_hikari_active`, `demo_hikari_idle`, `demo_hikari_total`
* HTTP worker/queue/in-flight gauges and logical connection lifecycle counters
* `demo_postgres_cpu_percent`, `demo_postgres_memory_percent`, and resource sample
  timestamps, forwarded from actual Docker stats by the host-side sampler
* PostgreSQL exporter `pg_stat_database_numbackends`, commits and tuple activity
* OJP's actual `ojp_hikari_pool_connections_*` gauges, including
  `active_connections`, `idle_connections`, `max_connections`, `pending_threads`
* `ojp_sql_executions_total` and `ojp_sql_slow_executions_total`
  (`sql_statement` label); the latter is absent until a slow execution is recorded

OJP's dotted OpenTelemetry instrument names are **not** its scrape names:
Prometheus adds unit suffixes. Never substitute invented OJP throttling counters
for metrics missing from this release. Server metrics are server/pool scoped,
not labeled with benchmark phases; interpret them only within the recorded
measurement window.

OJP 1.0.3 does **not** export dedicated admission-rejection, fast/slow-lane,
or JDBC client-throttle Prometheus instruments. Its managers expose internal
state and logs, but these are not scrapeable counters. The dashboard therefore
shows real backend pressure and SQL metrics instead of fabricated lane/rejection
numbers. This release limitation prevents a dedicated OJP-throttling graph;
client errors/drops and server logs must accompany the pool measurements.

Application histograms carry bounded mode/operation/phase/scenario/RPS labels.
Run IDs identify exported files, not an unbounded Prometheus label. Select the
result's **absolute UTC measurement window** in Grafana to avoid mixing runs;
also select `phase=measure`, mode, scenario and level. Gauge zeroes during the
inactive connection mode are expected. Histogram quantiles need enough samples
and scrapes; a very short integration interval is not a reliable P99 estimate.
Choose a new run ID for each invocation; existing result files are not silently
reused or merged into a new run.

JSON and CSV in `results/` retain each run/mode/level, warm-up and measured counts,
per-operation proportions and latency percentiles, errors, configuration, and
connection-readiness snapshots. CPU/memory evidence is captured with Docker stats,
retained in JSONL, and graphed through the application metrics endpoint. CPU 100%
means one core, not the whole host; memory percentage uses the container limit.
Samples older than ten seconds are filtered from the resource panels rather than
being displayed as current data. Do not equate offered RPS with successful SQL throughput or report
only successful latency while ignoring timeouts/drops.

When comparing, report host and container CPU/memory, database capacity and
active sessions, dataset/seed, arrival rate, max in flight, connection counts,
phase durations, transaction failures, drain duration and client saturation.
Use both latency and successful throughput. Repeat runs, alternate ordering,
and preserve raw results. This setup provides measurements, not a claim that
OJP is universally faster.

## Automated validation

```bash
bash scripts/test-integration.sh
```

Integration tests use an isolated Compose project and a reduced deterministic
dataset, but retain the **standard 200-connection direct pool** unless an explicit
low-resource override is selected. They run real PostgreSQL, OJP and the
packaged Java application—not mocks. The suite covers infrastructure and data,
both transaction paths and ordered ramps, open-loop scheduling, slow-query mix,
warm-up isolation, real node failure, telemetry/provisioning, mode isolation,
parameter validation, 200 simultaneous no-DB HTTP requests, resource lifecycle
and forced rollback. It captures failure diagnostics and returns nonzero on
failed assertions. Cleanup targets only its own Compose project.

Standalone targeted checks:

```bash
JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64 \
  PATH=/usr/lib/jvm/temurin-21-jdk-amd64/bin:$PATH mvn -f app/pom.xml package
java -jar app/target/demo.jar --validate-config
python3 -m unittest discover -s load -p 'test_*.py'
docker compose config --quiet
```

The JAR includes dependencies and JDBC service entries via Maven Shade's
`ServicesResourceTransformer` and sets `Main-Class: demo.Main` via
`ManifestResourceTransformer`. Integration runs the packaged container rather
than relying solely on compilation.

## Troubleshooting the eight historical regressions

1. **`unknown shorthand flag: 'd' in -d`:** verify `docker compose version`,
   install `docker-compose-plugin` from Docker's Ubuntu repository, and check
   daemon permissions. Scripts fail preflight before attempting startup.
2. **`no main manifest attribute`:** rebuild `app/` with Maven/Compose. Check
   `unzip -p app/target/demo.jar META-INF/MANIFEST.MF`; it must name `demo.Main`.
   Do not substitute an unshaded dependency-free JAR.
3. **Wrong paths:** application sources are under `app/src/main/java/demo`,
   Maven under `app/pom.xml`, Dockerfile under `app/Dockerfile`; Python load
   generation is under `load/runner.py`. Scripts locate the repository themselves.
4. **Application exits:** inspect `docker compose ps -a` and
   `docker compose logs app ojp postgres`; Compose startup alone is not readiness.
   Check `/ready` diagnostics and seeded schema.
5. **All requests fail:** the runner gates on health, readiness and real SQL.
   Check application URL, network, mode-switch error and database credentials
   before generating traffic. A result with zero SQL completions is a failure.
6. **No Grafana dashboard:** check `/api/dashboards/uid/ojp-benchmark`, provisioning
   mount paths and Grafana logs. The dashboard is checked automatically.
7. **Dashboard has no data:** inspect Prometheus `/targets`, `/metrics` on the
   application and OJP, choose the correct UTC window/phase/mode/level, and
   generate real workload. OJP pool series appear only after a datasource opens.
8. **Direct pool fails to reach 200:** check `/ready`, Hikari active/idle/total,
   PostgreSQL `SHOW max_connections`, `pg_stat_activity` by `application_name`,
   container memory/CPU and PostgreSQL logs. Increase the explicit startup
   timeout for slow hosts; do **not** silently shrink the standard pool.
   Exporter/admin sessions also consume connections.

Useful diagnostic SQL:

```sql
SHOW max_connections;
SELECT application_name, state, count(*)
FROM pg_stat_activity WHERE datname = 'shop'
GROUP BY application_name, state ORDER BY application_name, state;
SELECT * FROM benchmark_seed;
```

If Docker resolves a service alias to an address different from
`docker inspect`, inspect both containers' networks and DNS. During development
an engine-side stale alias was repaired by `docker compose down` followed by
startup, preserving named volumes. Do not hardcode container IPs in JDBC URLs.
For Maven `PKIX path building failed`, use the trusted build CA secret described
above rather than disabling TLS checks.

## Known limitations

This is a trusted local benchmark, not a hardened multi-tenant service. Dataset
reset restores logical rows, not disk layout, vacuum history or cold caches.
The single-database mapping differs from ShopService's two datasource domains.
Review writes append instead of updating seeded reviews to permit exact resets.
Revenue uses current catalog prices because upstream order items have no
historical price column. HTTP socket timeouts can leave ambiguous commit status.
OJP server metrics are aggregate and may describe pools retained between runs;
recreate OJP for a completely clean server process. HA tests allow transient
errors and cannot demonstrate durability of ambiguous in-flight writes. These
limitations must accompany any published performance results.