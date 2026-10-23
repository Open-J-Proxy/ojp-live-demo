import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.*;
import java.util.regex.*;

// Java 25 source launcher; JDBC application sources remain Java 21.
final String[] MODES = {"direct", "ojp"};
final String[] TABLES = {"users", "products", "orders", "order_items", "reviews"};
Environment E;

record Metric(Map<String, String> labels, double value) {}

List<Metric> metricRows(String text, String name) {
    var pattern = Pattern.compile("^" + Pattern.quote(name) + "(?:\\{([^}]*)\\})?\\s+([0-9.eE+-]+)$");
    var result = new ArrayList<Metric>();
    for (String line : text.split("\\R")) {
        var match = pattern.matcher(line);
        if (!match.matches()) continue;
        var labels = new LinkedHashMap<String, String>();
        var label = Pattern.compile("(\\w+)=\"([^\"]*)\"").matcher(Objects.toString(match.group(1), ""));
        while (label.find()) labels.put(label.group(1), label.group(2));
        result.add(new Metric(labels, Double.parseDouble(match.group(2))));
    }
    return result;
}

double total(String text, String name, Map<String, ?> labels) {
    return metricRows(text, name).stream().filter(row -> labels.entrySet().stream()
        .allMatch(e -> Objects.equals(row.labels().get(e.getKey()), Objects.toString(e.getValue()))))
        .mapToDouble(Metric::value).sum();
}

@SuppressWarnings("unchecked")
Map<String, Object> map(Object value) {
    require(value instanceof Map, "Expected object: " + value);
    return (Map<String, Object>) value;
}
@SuppressWarnings("unchecked")
List<Object> list(Object value) {
    require(value instanceof List, "Expected array: " + value);
    return (List<Object>) value;
}
Object at(Object object, String... keys) {
    Object result = object;
    for (String key : keys) result = map(result).get(key);
    return result;
}
double number(Object value) {
    require(value instanceof Number, "Expected number: " + value);
    return ((Number) value).doubleValue();
}
void require(boolean condition, Object detail) {
    if (!condition) throw new AssertionError(Objects.toString(detail));
}
void equal(Object actual, Object expected) { equal(actual, expected, ""); }
void equal(Object actual, Object expected, Object detail) {
    boolean same = actual instanceof Number && expected instanceof Number
        ? Double.compare(number(actual), number(expected)) == 0 : Objects.equals(actual, expected);
    require(same, "Expected " + expected + ", got " + actual + " " + detail);
}
void greater(Object actual, double bound) { require(number(actual) > bound, actual + " must exceed " + bound); }
void atLeast(Object actual, double bound) { require(number(actual) >= bound, actual + " must be >= " + bound); }
void atMost(Object actual, double bound) { require(number(actual) <= bound, actual + " must be <= " + bound); }
boolean truth(Object value) {
    if (value == null || Boolean.FALSE.equals(value)) return false;
    if (value instanceof Collection<?> c) return !c.isEmpty();
    if (value instanceof Map<?, ?> m) return !m.isEmpty();
    if (value instanceof String s) return !s.isEmpty();
    return !(value instanceof Number n) || n.doubleValue() != 0;
}
void absent(Object value, String key) { require(!map(value).containsKey(key), value); }
void write(Path path, Object value) throws IOException {
    Files.writeString(path, LoadJson.stringify(value) + "\n");
}
String encode(Object value) { return URLEncoder.encode(value.toString(), StandardCharsets.UTF_8); }
void sleep(double seconds) throws InterruptedException { Thread.sleep((long) (seconds * 1000)); }
long deadline(double seconds) { return System.nanoTime() + (long) (seconds * 1e9); }

class Options {
    boolean keep, small = "true".equalsIgnoreCase(System.getenv("IT_SMALL"));
    List<String> only = new ArrayList<>();
    double rampDuration = Double.parseDouble(System.getenv().getOrDefault("IT_RAMP_DURATION", "10"));
}

class Environment implements AutoCloseable {
    final Options options;
    final Path root = DemoTools.root();
    final Path script = root.resolve("scripts");
    final String project = "ojp-it-" + ProcessHandle.current().pid();
    final Path output = root.resolve("results/integration").resolve(project);
    final Map<String, String> env = new HashMap<>(System.getenv());
    final int sessions;
    final PrintWriter commands;
    final AtomicBoolean finalized = new AtomicBoolean();
    final CountDownLatch finished = new CountDownLatch(1);
    volatile boolean stopping;
    volatile Thread cleanupOwner;
    String base;
    DemoTools.Bridge bridge;

    Environment(Options options) throws Exception {
        this.options = options;
        sessions = options.small ? 20 : 200;
        Files.createDirectories(output);
        env.putAll(Map.of("COMPOSE_PROJECT_NAME", project, "TEST_ENDPOINTS", "true",
            "DIRECT_POOL_SIZE", "" + sessions, "HTTP_WORKERS", "512", "PG_MAX_CONNECTIONS", "300",
            "START_TIMEOUT", "300", "SLOW_QUERY_ENABLED", "true", "SLOW_QUERY_THRESHOLD_MS", "1"));
        env.putAll(Map.of("SEED_USERS", "200", "SEED_PRODUCTS", "100", "SEED_ORDERS", "1000",
            "SEED_ITEMS", "3000", "SEED_REVIEWS", "500", "SEED", "12345",
            "RESULTS_DIR", output.toString()));
        env.put("LOCAL_UID", DemoTools.run(List.of("id", "-u"), env, 10).output().trim());
        env.put("LOCAL_GID", DemoTools.run(List.of("id", "-g"), env, 10).output().trim());
        for (String name : List.of("APP_PORT", "POSTGRES_PORT", "PROMETHEUS_PORT", "GRAFANA_PORT")) env.put(name, "0");
        for (String name : List.of("APP_URL", "OJP_URL", "OJP_HA_URL", "COMPOSE_COMPATIBILITY")) env.remove(name);
        String postgres = project + "-postgres-1", primary = project + "-ojp-1";
        String backend = "_postgresql://" + postgres + ":5432/shop?ApplicationName=demo-ojp";
        env.put("APP_INTERNAL_URL", "http://" + project + "-app-1:8080");
        env.put("OJP_HA_URL", "jdbc:ojp[" + String.join(",", List.of("ojp", "ojp2", "ojp3").stream()
            .map(node -> project + "-" + node + "-1:1059").toList()) + "]" + backend);
        var scrapes = new ArrayList<Object>();
        for (var entry : Map.of("app", project + "-app-1:8081", "postgres", project + "-postgres-exporter-1:9187",
                "ojp", primary + ":9159", "prometheus", "localhost:9090").entrySet())
            scrapes.add(Map.of("job_name", entry.getKey(), "static_configs", List.of(Map.of("targets", List.of(entry.getValue())))));
        scrapes.add(Map.of("job_name", "ojp-ha", "dns_sd_configs", List.of(Map.of(
            "names", List.of(project + "-ojp2-1", project + "-ojp3-1"), "type", "A", "port", 9159, "refresh_interval", "5s"))));
        Path prom = output.resolve("prometheus.json"), override = output.resolve("compose.override.json");
        write(prom, Map.of("global", Map.of("scrape_interval", "2s", "evaluation_interval", "2s"), "scrape_configs", scrapes));
        write(override, Map.of("services", Map.of(
            "app", Map.of("environment", Map.of("PG_URL", "jdbc:postgresql://" + postgres + ":5432/shop",
                "OJP_URL", "${OJP_URL:-jdbc:ojp[" + primary + ":1059]" + backend + "}")),
            "postgres-exporter", Map.of("environment", Map.of("DATA_SOURCE_NAME",
                "postgresql://demo:${DB_PASSWORD:-demo-local-only}@" + postgres + ":5432/shop?sslmode=disable")),
            "prometheus", Map.of("volumes", List.of(prom + ":/etc/prometheus/prometheus.yml:ro")))));
        env.put("COMPOSE_OVERRIDE_FILE", override.toString());
        commands = new PrintWriter(Files.newBufferedWriter(output.resolve("commands.log")), true);
    }
    String run(List<String> args, int timeout, boolean check) throws Exception {
        if (stopping && Thread.currentThread() != cleanupOwner) throw new InterruptedException("Integration is shutting down");
        commands.println("$ " + String.join(" ", args));
        var result = DemoTools.run(args, env, timeout);
        commands.print(result.output()); commands.flush();
        if (check && result.exitCode() != 0) throw new IOException("Command exited " + result.exitCode()
            + ": " + String.join(" ", args) + "\n" + result.output().substring(Math.max(0, result.output().length() - 12000)));
        return result.output();
    }
    String run(List<String> args) throws Exception { return run(args, 600, true); }
    String shell(String name, String... args) throws Exception {
        var command = new ArrayList<>(List.of(script.resolve(name).toString()));
        command.addAll(List.of(args)); return run(command);
    }
    String compose(int timeout, boolean check, String... args) throws Exception {
        var command = new ArrayList<>(DemoTools.compose(env)); command.addAll(List.of(args));
        return run(command, timeout, check);
    }
    String compose(String... args) throws Exception { return compose(600, true, args); }
    String sql(String query) throws Exception {
        return compose("exec", "-T", "postgres", "sh", "-ec",
            "exec psql -X -v ON_ERROR_STOP=1 -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\" -Atc \"$1\"", "psql", query).trim();
    }
    String serviceUrl(String service, int port) throws Exception {
        return bridge != null ? "http://" + project + "-" + service + "-1:" + port
            : "http://" + compose("port", service, "" + port).trim();
    }
    void refreshBase() throws Exception { base = serviceUrl("app", 8080); }
    List<Long> counts() throws Exception {
        return Arrays.stream(sql("SELECT " + String.join(", ", Arrays.stream(TABLES)
            .map(table -> "(SELECT count(*) FROM " + table + ")").toList())).split("\\|")).map(Long::parseLong).toList();
    }
    DemoTools.Response call(String path) throws Exception { return call(path, null, null, 60, null, Map.of()); }
    DemoTools.Response call(String path, Object data, String method) throws Exception {
        return call(path, data, method, 60, null, Map.of());
    }
    DemoTools.Response call(String path, Object data, String method, int timeout, String base, Map<String, String> headers) throws Exception {
        if (stopping && Thread.currentThread() != cleanupOwner) throw new InterruptedException("Integration is shutting down");
        var all = new HashMap<>(headers);
        all.put("Content-Type", "application/json");
        if ("POST".equals(method) && truth(env.get("ADMIN_TOKEN"))) all.put("Authorization", "Bearer " + env.get("ADMIN_TOKEN"));
        String url = Objects.requireNonNullElse(base, this.base) + path;
        return bridge != null ? bridge.request(url, data, method, timeout, all)
            : DemoTools.http(url, data, method, timeout, all);
    }
    void mode(String selected) throws Exception {
        var response = call("/mode?value=" + selected, Map.of(), "POST"); equal(response.status(), 200, response.body());
    }
    void reset() throws Exception {
        var response = call("/reset", Map.of(), "POST", 120, null, Map.of()); equal(response.status(), 200, response.body());
    }
    DemoTools.Response work(String op, Object... parameters) throws Exception {
        var query = new LinkedHashMap<String, Object>(Map.of("op", op, "seed", 12345, "phase", "measure", "scenario", "integration", "level", 1));
        for (int i = 0; i < parameters.length; i += 2) query.put(parameters[i].toString(), parameters[i + 1]);
        return call("/work?" + String.join("&", query.entrySet().stream().map(e -> encode(e.getKey()) + "=" + encode(e.getValue())).toList()));
    }
    String metrics() throws Exception {
        var response = call("/metrics"); equal(response.status(), 200, response.body());
        require(response.body() instanceof String, response.body()); return (String) response.body();
    }
    List<Map<String, Object>> results(String id) throws Exception {
        try (var paths = Files.list(output)) {
            var result = new ArrayList<Map<String, Object>>();
            for (Path path : paths.filter(p -> p.getFileName().toString().startsWith(id + "-") && p.toString().endsWith(".json")).sorted().toList())
                result.add(map(LoadJson.parse(Files.readString(path))));
            return result;
        }
    }
    List<Map<String, Object>> runner(String id, String... options) throws Exception {
        var sampler = new Resources.ResourceSampler(output, id, env);
        sampler.start();
        try {
            var args = new ArrayList<>(List.of("--profile", "tools", "run", "--rm", "--user",
                env.get("LOCAL_UID") + ":" + env.get("LOCAL_GID"), "load", "--url", env.get("APP_INTERNAL_URL"),
                "--scenario", "integration", "--run-id", id, "--output", "/results"));
            args.addAll(List.of(options)); compose(args.toArray(String[]::new));
        } finally { sampler.stop(); }
        var results = results(id); require(!results.isEmpty(), "Runner created no JSON results"); return results;
    }
    void diagnostics(String label) {
        String safe = label.replaceAll("[^a-zA-Z0-9_.-]", "_");
        int timeout = stopping ? 5 : 60;
        for (String suffix : List.of("ps", "logs")) {
            try {
                String text = suffix.equals("ps") ? compose(timeout, false, "--profile", "ha", "--profile", "tools", "ps", "-a")
                    : compose(timeout, false, "--profile", "ha", "--profile", "tools", "logs", "--no-color", "--tail", "300");
                Files.writeString(output.resolve(safe + "-" + suffix + ".log"), text);
            } catch (Exception error) {
                try { Files.writeString(output.resolve(safe + "-" + suffix + ".log"), error.toString()); } catch (IOException ignored) {}
            }
        }
        try {
            var response = call("/metrics", null, null, timeout, null, Map.of());
            equal(response.status(), 200, response.body());
            Files.writeString(output.resolve(safe + "-metrics.txt"), response.body().toString());
            Files.writeString(output.resolve(safe + "-sessions.txt"), compose(timeout, false, "exec", "-T", "postgres", "sh", "-ec",
                "exec psql -X -v ON_ERROR_STOP=1 -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\" -Atc \"$1\"", "psql",
                "SELECT pid,application_name,state,wait_event,query FROM pg_stat_activity"));
        } catch (Exception ignored) {}
    }
    public void close() {
        if (bridge != null) {
            try { bridge.close(); } catch (Exception error) { System.err.println("HTTP bridge cleanup failed: " + error); }
            bridge = null;
        }
        if (!options.keep) {
            try { compose(stopping ? 60 : 180, true, "--profile", "ha", "--profile", "tools", "down", "--volumes", "--remove-orphans"); }
            catch (Exception error) { System.err.println("Cleanup failed for " + project + ": " + error); }
        } else System.out.println("Preserved project: " + project);
        commands.close();
    }
    void finish(Map<String, Object> summary, boolean succeeded) {
        if (!finalized.compareAndSet(false, true)) {
            if (stopping) {
                try { finished.await(100, TimeUnit.SECONDS); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            }
            return;
        }
        cleanupOwner = Thread.currentThread();
        try {
            if (stopping) {
                summary.put("passed", false);
                summary.put("interrupted", true);
                summary.put("startup_error", "Integration interrupted by JVM shutdown");
            }
            if (!succeeded || stopping) diagnostics(stopping ? "interrupted" : "final-failure");
            if (stopping) {
                try {
                    String ids = run(List.of("docker", "ps", "-aq", "--filter", "label=com.docker.compose.project=" + project,
                        "--filter", "label=com.docker.compose.service=load"), 5, true).trim();
                    if (!ids.isEmpty()) {
                        var remove = new ArrayList<>(List.of("docker", "rm", "-f"));
                        remove.addAll(Arrays.asList(ids.split("\\s+")));
                        run(remove, 10, false);
                    }
                } catch (Exception error) { System.err.println("Workload cleanup failed: " + error); }
            }
            try { write(output.resolve("integration-summary.json"), summary); }
            catch (IOException error) { System.err.println("Cannot write integration summary: " + error); }
            finally { close(); }
        } finally { finished.countDown(); }
    }
}

Map<String, Object> success(DemoTools.Response response) {
    equal(response.status(), 200, response.body());
    var body = map(response.body());
    require(truth(body.getOrDefault("ok", body.getOrDefault("success", false))), body);
    require(!Boolean.FALSE.equals(body.get("success")), body);
    require(!Boolean.FALSE.equals(body.get("sql_completed")), body);
    return body;
}

void test_00_infrastructure() throws Exception {
    var services = new ArrayList<Object>();
    for (String line : E.compose("ps", "--format", "json").split("\\R")) {
        Object value = LoadJson.parse(line);
        if (value instanceof List) services.addAll(list(value)); else services.add(value);
    }
    var running = new HashSet<>();
    for (Object item : services) if ("running".equals(at(item, "State"))) running.add(at(item, "Service"));
    require(running.containsAll(List.of("postgres", "ojp", "app", "prometheus", "grafana", "postgres-exporter")), services);
    equal(E.sql("SELECT 1"), "1"); equal(E.call("/health").status(), 200); require(truth(at(E.call("/ready").body(), "ready")), "not ready");
}

void test_01_seed_cardinality_and_foreign_keys() throws Exception {
    E.reset(); equal(E.counts(), List.of(200L, 100L, 1000L, 3000L, 500L));
    equal(E.sql("SELECT seed,users,products,orders,items,reviews FROM benchmark_seed"), "12345|200|100|1000|3000|500");
    equal(E.sql("SELECT count(*) FROM order_items i LEFT JOIN orders o ON o.id=i.order_id LEFT JOIN products p ON p.id=i.product_id WHERE o.id IS NULL OR p.id IS NULL"), "0");
    equal(E.sql("SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace AND contype='f' AND convalidated"), "5");
    var indexes = Set.of(E.sql("SELECT indexname FROM pg_indexes WHERE schemaname='public'").split("\\R"));
    require(indexes.containsAll(List.of("orders_user_date", "orders_date", "items_order", "items_product", "reviews_product", "reviews_user", "products_price")), indexes);
    equal(E.sql("SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace AND contype='c' AND convalidated"), "4");
    E.shell("seed-data.sh"); equal(E.counts(), List.of(200L, 100L, 1000L, 3000L, 500L));
}

void test_02_health_and_mode_real_queries() throws Exception {
    for (String mode : MODES) { E.mode(mode); equal(E.call("/health").status(), 200); equal(E.call("/ready").status(), 200); success(E.work("browse")); }
    equal(E.call("/mode?value=invalid", Map.of(), "POST").status(), 400); E.shell("verify-connections.sh");
}

void test_03_all_read_operations_both_modes() throws Exception {
    var rows = new HashMap<String, Map<String, Object>>();
    var failures = new ArrayList<String>();
    for (String mode : MODES) {
        E.mode(mode); E.reset(); rows.put(mode, new HashMap<>());
        for (String op : List.of("browse", "history", "account", "report")) {
            try { rows.get(mode).put(op, success(E.work(op)).get("rows")); }
            catch (AssertionError | Exception error) { failures.add(mode + "/" + op + ": " + error); }
        }
    }
    require(failures.isEmpty(), failures); equal(rows.get("direct"), rows.get("ojp"), "Same baseline/seed must return consistent SQL row counts");
}

void test_04_committed_checkout_review_counts() throws Exception {
    for (String mode : MODES) {
        E.mode(mode); E.reset(); var baseline = E.counts(); success(E.work("checkout")); success(E.work("review")); var changed = E.counts();
        equal(changed.subList(0, 2), baseline.subList(0, 2)); equal(changed.get(2), baseline.get(2) + 1);
        greater(changed.get(3), baseline.get(3)); equal(changed.get(4), baseline.get(4) + 1);
    }
    E.reset();
}

void test_05_transaction_rollback_both_modes() throws Exception {
    for (String mode : MODES) {
        E.mode(mode); var before = E.counts();
        var response = E.call("/rollback?seed=12345&phase=measure&scenario=integration&level=1", Map.of(), "POST");
        equal(response.status(), 200, response.body()); var body = map(response.body());
        require(truth(body.getOrDefault("rolledBack", body.get("rollback"))), body); equal(E.counts(), before);
    }
}

void test_06_reset_restores_replay_baseline() throws Exception {
    E.mode("direct"); E.shell("reset.sh"); var baseline = E.counts();
    for (int i = 0; i < 2; i++) {
        success(E.work("checkout", "seed", 9876)); success(E.work("review", "seed", 9876));
        require(!E.counts().equals(baseline), "writes did not change baseline"); E.reset(); equal(E.counts(), baseline);
    }
}

List<Future<DemoTools.Response>> concurrent(ExecutorService executor, int count, Callable<DemoTools.Response> work) throws Exception {
    var barrier = new CyclicBarrier(count + 1);
    var futures = new ArrayList<Future<DemoTools.Response>>();
    for (int i = 0; i < count; i++) futures.add(executor.submit(() -> { barrier.await(30, TimeUnit.SECONDS); return work.call(); }));
    barrier.await(30, TimeUnit.SECONDS); return futures;
}

void test_07_direct_200_simultaneous_physical_sessions() throws Exception {
    E.mode("direct");
    String applicationName = at(E.call("/ready").body(), "direct_application_name").toString();
    int physical = Integer.parseInt(E.sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='" + applicationName + "'"));
    equal(physical, Integer.parseInt(E.env.get("DIRECT_POOL_SIZE")));
    double opened = total(E.metrics(), "demo_logical_connections_opened_total", Map.of("mode", "direct"));
    int peak = 0;
    try (var executor = Executors.newFixedThreadPool(E.sessions)) {
        var futures = concurrent(executor, E.sessions, () -> E.call("/hold?millis=10000", null, null, 90, null, Map.of()));
        long end = deadline(9);
        while (System.nanoTime() < end) {
            peak = Math.max(peak, (int) total(E.metrics(), "demo_hold_active", Map.of()));
            if (peak >= E.sessions) break; sleep(.1);
        }
        for (var future : futures) success(future.get());
    }
    write(E.output.resolve("direct-sessions.json"), Map.of("expected_http_concurrency", E.sessions, "observed_http_peak", peak,
        "physical_sessions", physical, "small_profile", E.options.small));
    atLeast(peak, E.sessions);
    equal(total(E.metrics(), "demo_logical_connections_opened_total", Map.of("mode", "direct")), opened, "/hold must not manufacture database work");
}

void test_08_logical_connection_lifecycle_balanced() throws Exception {
    for (String mode : MODES) { E.mode(mode); for (int i = 0; i < 10; i++) success(E.work("browse")); }
    String metrics = E.metrics();
    require(!metricRows(metrics, "demo_logical_connections_opened_total").isEmpty() && !metricRows(metrics, "demo_logical_connections_closed_total").isEmpty(), metrics);
    for (String mode : MODES) {
        double opened = total(metrics, "demo_logical_connections_opened_total", Map.of("mode", mode));
        greater(opened, 0); equal(opened, total(metrics, "demo_logical_connections_closed_total", Map.of("mode", mode)), mode);
    }
}

void test_09_ojp_multiplexes_bounded_physical_pool() throws Exception {
    E.mode("ojp"); equal(E.sql("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'demo-direct-%'"), "0");
    for (String name : List.of("demo_hikari_total", "demo_hikari_active", "demo_hikari_idle")) equal(metricRows(E.metrics(), name).getFirst().value(), 0);
    int concurrency = E.options.small ? 20 : 60, peak = 0, successes = 0, rejected = 0;
    var labels = Map.of("mode", "ojp", "op", "report", "scenario", "htap");
    double before = total(E.metrics(), "demo_completions_total", labels);
    try (var executor = Executors.newFixedThreadPool(concurrency)) {
        var futures = concurrent(executor, concurrency, () -> E.work("report", "scenario", "htap"));
        while (futures.stream().anyMatch(f -> !f.isDone())) {
            peak = Math.max(peak, Integer.parseInt(E.sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='demo-ojp'"))); sleep(.1);
        }
        for (var future : futures) {
            var response = future.get();
            if (response.status() == 200) { success(response); successes++; }
            else { equal(response.status(), 503, response.body()); equal(at(response.body(), "code"), "08001", response.body()); rejected++; }
        }
    }
    atLeast(peak, 10); greater(successes, 0); equal(successes + rejected, concurrency);
    equal(total(E.metrics(), "demo_completions_total", labels) - before, successes);
    int limit = Integer.parseInt(E.env.getOrDefault("OJP_POOL_SIZE", "10")); atMost(peak, limit);
    if (!E.options.small) require(peak < concurrency, peak);
    write(E.output.resolve("ojp-sessions.json"), Map.of("logical_concurrency", concurrency, "physical_peak", peak, "physical_limit", limit,
        "sql_successes", successes, "pool_acquisition_rejections", rejected));
}

void test_10_phase_histograms_and_level_sql_completions() throws Exception {
    E.mode("direct"); String before = E.metrics();
    for (String phase : List.of("warmup", "measure")) for (int level : List.of(17, 23))
        for (int i = 0; i < 3; i++) success(E.work("browse", "phase", phase, "level", level));
    String after = E.metrics();
    for (String phase : List.of("warmup", "measure")) for (String level : List.of("17", "23"))
        for (String name : List.of("demo_completions_total", "demo_sql_duration_seconds_count", "demo_request_duration_seconds_count", "demo_acquisition_duration_seconds_count")) {
            var labels = Map.of("mode", "direct", "op", "browse", "scenario", "integration", "phase", phase, "level", level);
            equal(total(after, name, labels) - total(before, name, labels), 3, phase + "/" + level + "/" + name);
        }
    require(metricRows(after, "demo_request_duration_seconds_bucket").stream().anyMatch(row -> "+Inf".equals(row.labels().get("le")) && row.value() > 0), after);
}

List<Map<String, String>> csv(Path path) throws IOException {
    var records = new ArrayList<List<String>>();
    var fields = new ArrayList<String>(); var field = new StringBuilder(); boolean quoted = false;
    String text = Files.readString(path);
    for (int i = 0; i < text.length(); i++) {
        char c = text.charAt(i);
        if (c == '"') {
            if (quoted && i + 1 < text.length() && text.charAt(i + 1) == '"') { field.append('"'); i++; }
            else quoted = !quoted;
        } else if (!quoted && (c == ',' || c == '\n' || c == '\r')) {
            fields.add(field.toString()); field.setLength(0);
            if (c != ',') { records.add(fields); fields = new ArrayList<>(); if (c == '\r' && i + 1 < text.length() && text.charAt(i + 1) == '\n') i++; }
        } else field.append(c);
    }
    require(!quoted, "Unterminated CSV quote: " + path);
    if (!fields.isEmpty() || !field.isEmpty()) { fields.add(field.toString()); records.add(fields); }
    require(!records.isEmpty(), "Empty CSV: " + path);
    var header = records.removeFirst(); var rows = new ArrayList<Map<String, String>>();
    for (var record : records) {
        equal(record.size(), header.size(), path);
        var row = new LinkedHashMap<String, String>();
        for (int i = 0; i < header.size(); i++) row.put(header.get(i), record.get(i));
        rows.add(row);
    }
    return rows;
}

Map<String, Object> resultLabels(Map<String, Object> item, String phase) {
    return Map.of("mode", item.get("mode"), "phase", phase, "scenario", item.get("scenario"), "level", item.get("level"));
}
double completions(Object snapshot, Map<String, Object> item, String phase) {
    return total(at(snapshot, "response").toString(), "demo_completions_total", resultLabels(item, phase));
}
List<Object> pairs(List<Map<String, Object>> results) {
    return results.stream().map(item -> (Object) List.of(item.get("mode"), item.get("rps"))).toList();
}

void test_11_ordered_sequential_comparison_and_metadata() throws Exception {
    var results = E.runner("it-compare", "--mode", "both", "--rps", "3,7", "--warmup", "1", "--duration", "5",
        "--seed", "4242", "--max-in-flight", "17", "--request-timeout", "45");
    equal(pairs(results).toString(), List.of(List.of("direct", 3), List.of("direct", 7), List.of("ojp", 3), List.of("ojp", 7)).toString());
    for (int index = 0; index < results.size(); index++) {
        var item = results.get(index);
        absent(item, "error"); equal(item.get("seed"), 4242); equal(item.get("max_in_flight"), 17);
        equal(item.get("request_timeout"), 45); equal(item.get("scenario"), "integration"); equal(item.get("run_id"), "it-compare");
        equal(at(item, "readiness_before_measure", "/health", "status"), 200);
        equal(at(item, "readiness_before_measure", "/ready", "status"), 200);
        require(truth(at(item, "readiness_before_measure", "work_probe", "sql_completed")), item);
        equal(at(item, "readiness_after", "status"), 200);
        equal(at(item, "metrics_before_measure", "status"), 200); equal(at(item, "metrics_after", "status"), 200);
        require(truth(item.get("benchmark_result_published")), item);
        for (String phase : List.of("warmup", "measure")) {
            int duration = phase.equals("warmup") ? 1 : 5;
            var report = map(item.get(phase)); var counts = map(report.get("counts"));
            atMost(report.get("start_epoch_seconds"), number(report.get("end_arrivals_epoch_seconds")));
            atMost(report.get("end_arrivals_epoch_seconds"), number(report.get("end_epoch_seconds")));
            require(Math.abs(number(report.get("end_arrivals_epoch_seconds")) - number(report.get("start_epoch_seconds")) - duration) < .00005, report);
            require(report.get("start_time").toString().endsWith("Z") && report.get("end_time").toString().endsWith("Z"), report);
            equal(counts.get("scheduled"), Math.ceil(number(item.get("rps")) * duration));
            equal(counts.get("sql_completed"), counts.get("scheduled"));
            equal(counts.get("successful_transactions"), counts.get("sql_completed"));
            equal(counts.get("failed_transactions"), 0); equal(counts.get("in_flight"), 0);
            require(at(report, "latency_ms", "end_to_end", "p99") != null, report);
            var telemetry = map(report.get("telemetry"));
            require(truth(telemetry.get("final_sent")), telemetry); greater(telemetry.get("samples_sent"), 0); require(!truth(telemetry.get("errors")), telemetry);
            var histogram = map(report.get("scheduled_latency_histogram"));
            equal(histogram.get("count"), counts.get("completed")); equal(histogram.get("count"), counts.get("dispatched"));
            equal(at(histogram, "buckets", "+Inf"), histogram.get("count"));
            var buckets = new ArrayList<>(map(histogram.get("buckets")).entrySet());
            buckets.sort(Comparator.comparingDouble(entry -> entry.getKey().equals("+Inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(entry.getKey())));
            double previous = Double.NEGATIVE_INFINITY;
            for (var bucket : buckets) { atLeast(bucket.getValue(), previous); previous = number(bucket.getValue()); }
            atLeast(histogram.get("sum_seconds"), 0);
        }
        equal(completions(item.get("metrics_after"), item, "measure") - completions(item.get("metrics_before_measure"), item, "measure"),
            at(item, "measure", "counts", "sql_completed"), "Per-level application SQL completions must match runner completions");
        Path file = E.output.resolve("it-compare-%03d-%s-%s.csv".formatted(index + 1, item.get("mode"), item.get("level")));
        require(Files.exists(file), file); var rows = csv(file); equal(rows.size(), 14);
        equal(rows.stream().map(row -> row.get("phase")).collect(java.util.stream.Collectors.toSet()), Set.of("warmup", "measure"));
        for (var row : rows) {
            equal(row.get("mode"), item.get("mode")); equal(row.get("run_id"), item.get("run_id"));
            equal(row.get("start_time"), at(item, row.get("phase"), "start_time"));
            if (row.get("operation").equals("all")) equal(Long.parseLong(row.get("sql_completed")), at(item, row.get("phase"), "counts", "sql_completed"));
        }
    }
    for (int i = 0; i < 2; i++) {
        var direct = map(at(results.get(i), "measure", "operations")); var ojp = map(at(results.get(i + 2), "measure", "operations"));
        equal(direct.keySet(), ojp.keySet());
        for (String op : direct.keySet()) equal(at(direct.get(op), "scheduled"), at(ojp.get(op), "scheduled"));
    }
    String metrics = E.metrics();
    for (var item : results) require(metricRows(metrics, "demo_benchmark_result_successful_rps").stream()
        .anyMatch(row -> resultLabels(item, "measure").entrySet().stream().allMatch(e -> Objects.equals(row.labels().get(e.getKey()), e.getValue().toString()))), item);
}

void test_12_throttling_admission_not_sql_completion() throws Exception {
    var results = E.runner("it-throttle", "--scenario", "integration", "--mode", "direct", "--rps", "1000",
        "--warmup", "0", "--duration", "2", "--max-in-flight", "1", "--request-timeout", "30");
    equal(results.size(), 1); var item = results.getFirst(); absent(item, "error"); var counts = map(at(item, "measure", "counts"));
    greater(counts.get("dropped"), 0); greater(counts.get("sql_completed"), 0);
    equal(counts.get("scheduled"), number(counts.get("dispatched")) + number(counts.get("dropped")));
    atMost(counts.get("peak_in_flight"), 1); equal(counts.get("accepted"), counts.get("sql_completed"));
    require(number(counts.get("sql_completed")) < number(counts.get("scheduled")), counts);
    E.mode("ojp"); var switched = E.call("/ready").body(); equal(at(switched, "direct_sessions"), 0, switched); equal(at(switched, "mode"), "ojp", switched);
}

List<Object> serverCounts() throws Exception {
    var response = E.call("/metrics", null, null, 60, E.serviceUrl("ojp", 9159), Map.of()); equal(response.status(), 200);
    String text = response.body().toString(); var executions = metricRows(text, "ojp_sql_executions_total");
    var slow = metricRows(text, "ojp_sql_slow_executions_total");
    require(!executions.isEmpty(), "Missing actual OJP SQL execution counter");
    return Arrays.asList(executions.stream().mapToDouble(Metric::value).sum(), slow.isEmpty() ? null : slow.stream().mapToDouble(Metric::value).sum());
}

void test_13_analytical_queries_execute_real_aggregations() throws Exception {
    E.mode("ojp"); var beforeServer = serverCounts(); var rows = new HashMap<String, Object>();
    for (String mode : MODES) {
        E.mode(mode); E.reset(); var labels = Map.of("mode", mode, "op", "report", "scenario", "htap");
        double before = total(E.metrics(), "demo_completions_total", labels);
        var result = success(E.work("report", "scenario", "htap")); rows.put(mode, result.get("rows"));
        greater(result.get("rows"), 0); equal(total(E.metrics(), "demo_completions_total", labels) - before, 1); E.reset();
    }
    equal(rows.get("direct"), rows.get("ojp")); var afterServer = serverCounts(); greater(afterServer.get(0), number(beforeServer.get(0)));
    if (afterServer.get(1) != null) { atLeast(afterServer.get(1), beforeServer.get(1) == null ? 0 : number(beforeServer.get(1))); atMost(afterServer.get(1), number(afterServer.get(0))); }
    else equal(beforeServer.get(1), null);
    write(E.output.resolve("ojp-sql-classification.json"), Map.of("configured_threshold_ms", 1, "before", beforeServer, "after", afterServer,
        "note", "Absent slow series means no observed classifications; no latency/sleep guarantee."));
}

void test_14_monitoring_targets_and_exported_data() throws Exception {
    Path resources = E.output.resolve("it-compare/resources.jsonl");
    if (Files.exists(resources)) {
        var postgres = new ArrayList<Map<String, Object>>();
        for (String line : Files.readAllLines(resources)) {
            var record = map(LoadJson.parse(line));
            if (Objects.toString(map(record.getOrDefault("stats", Map.of())).get("Name"), "").endsWith("-postgres-1")) postgres.add(record);
        }
        require(!postgres.isEmpty(), "No real PostgreSQL Docker resource statistics");
        require(postgres.stream().anyMatch(row -> list(row.getOrDefault("workload_intervals", List.of())).stream()
            .anyMatch(interval -> "measure".equals(at(interval, "phase")) && truth(at(interval, "arrival_window_overlap")))),
            "No PostgreSQL CPU sample overlaps an actual measurement arrival window");
        for (var row : postgres) {
            equal(row.get("source"), "docker_stats"); require(row.containsKey("timestamp"), row);
            atLeast(Double.parseDouble(at(row, "stats", "CPUPerc").toString().replaceFirst("%$", "")), 0);
            require(map(row.get("stats")).containsKey("MemUsage"), row);
        }
    } else require(!E.options.only.isEmpty(), "Comparison did not persist PostgreSQL CPU evidence");
    String prometheus = E.serviceUrl("prometheus", 9090), grafana = E.serviceUrl("grafana", 3000);
    equal(E.call("/api/health", null, null, 60, grafana, Map.of()).status(), 200);
    String credentials = E.env.getOrDefault("GRAFANA_USER", "admin") + ":" + E.env.getOrDefault("GRAFANA_PASSWORD", "demo-local-only");
    var response = E.call("/api/dashboards/uid/ojp-benchmark", null, null, 60, grafana,
        Map.of("Authorization", "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8))));
    equal(response.status(), 200, response.body()); equal(at(response.body(), "dashboard", "uid"), "ojp-benchmark"); require(truth(at(response.body(), "dashboard", "panels")), response.body());
    long end = deadline(90); List<Object> targets = List.of(); var jobs = new HashMap<String, Object>(); var required = Set.of("app", "postgres", "ojp", "prometheus");
    while (System.nanoTime() < end) {
        response = E.call("/api/v1/targets", null, null, 60, prometheus, Map.of()); equal(response.status(), 200, response.body());
        targets = list(at(response.body(), "data", "activeTargets")); jobs.clear();
        for (Object target : targets) {
            String job = Objects.toString(at(target, "labels", "job"));
            if (required.contains(job)) jobs.put(job, at(target, "health"));
        }
        if (jobs.keySet().containsAll(required) && jobs.values().stream().allMatch("up"::equals)) break;
        sleep(2);
    }
    require(jobs.keySet().containsAll(required), targets); require(jobs.values().stream().allMatch("up"::equals), targets);
    write(E.output.resolve("prometheus-targets.json"), targets);
    response = E.call("/api/v1/query?query=" + encode("sum(demo_completions_total)"), null, null, 60, prometheus, Map.of());
    equal(response.status(), 200, response.body()); var result = list(at(response.body(), "data", "result")); require(!result.isEmpty(), response.body());
    greater(Double.parseDouble(list(at(result.getFirst(), "value")).get(1).toString()), 0);
    var dashboard = LoadJson.parse(Files.readString(E.root.resolve("monitoring/grafana/dashboards/benchmark.json")));
    for (Object panel : list(at(dashboard, "panels"))) for (Object target : list(map(panel).getOrDefault("targets", List.of()))) {
        String expression = at(target, "expr").toString().replace("$mode", "direct").replace("$phase", "measure").replace("$scenario", "htap").replace("$level", "16");
        response = E.call("/api/v1/query?query=" + encode(expression), null, null, 60, prometheus, Map.of());
        equal(response.status(), 200, expression + ": " + response.body()); equal(at(response.body(), "status"), "success", expression + ": " + response.body());
    }
}

void test_15_ha_failover_bounded_sql_recovery() throws Exception {
    E.run(List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(), E.root.resolve("load/Failover.java").toString(),
        "--run-id", "it-failover", "--rps", "10", "--warmup", "1", "--duration", "8", "--recovery-timeout", "60"));
    E.refreshBase(); var report = map(LoadJson.parse(Files.readString(E.output.resolve("it-failover-failover-events.json"))));
    require(truth(report.get("success")), report); var nodes = list(report.get("ha_nodes_before_fault"));
    equal(nodes.stream().map(node -> at(node, "service")).collect(java.util.stream.Collectors.toSet()), Set.of("ojp", "ojp2", "ojp3"));
    require(nodes.stream().allMatch(node -> "running".equals(at(node, "state"))), report);
    for (String key : List.of("fault_requested_at", "node_stopped_at", "recovered_at")) require(report.containsKey(key), report);
    var observations = list(report.get("observations")); greater(observations.size(), 2); atMost(report.get("recovery_seconds"), 60);
    require(observations.subList(observations.size() - 3, observations.size()).stream().allMatch(item -> truth(at(item, "sql_success"))), report);
    require(!truth(report.get("cleanup_errors")), report); require(E.compose("ps", "--format", "json", "ojp").toLowerCase(Locale.ROOT).contains("running"), "OJP not running");
}

void assertModeIsolation(Map<String, Object> item) {
    var readiness = at(item, "readiness_before_measure", "/ready", "response");
    equal(at(readiness, "mode"), item.get("mode"));
    equal(at(readiness, "direct_sessions"), "ojp".equals(item.get("mode")) ? 0 : Integer.parseInt(E.env.get("DIRECT_POOL_SIZE")));
    for (String name : List.of("demo_hikari_total", "demo_hikari_active", "demo_hikari_idle")) {
        var values = metricRows(at(item, "metrics_before_measure", "response").toString(), name); require(!values.isEmpty(), name);
        if ("ojp".equals(item.get("mode"))) equal(values.getFirst().value(), 0, name);
    }
}
void checkRamp(String mode) throws Exception {
    double duration = E.options.rampDuration;
    var results = E.runner("it-ramp-" + mode, "--mode", mode, "--rps", "2,4,8", "--warmup", "1", "--duration", "" + duration);
    equal(results.stream().map(item -> number(item.get("rps"))).toList(), List.of(2d, 4d, 8d));
    for (var item : results) {
        absent(item, "error"); equal(item.get("mode"), mode); var counts = map(at(item, "measure", "counts"));
        equal(counts.get("scheduled"), Math.ceil(number(item.get("rps")) * duration)); equal(counts.get("sql_completed"), counts.get("scheduled"));
        equal(counts.get("successful_transactions"), counts.get("scheduled")); equal(counts.get("failed_transactions"), 0); equal(counts.get("in_flight"), 0);
        assertModeIsolation(item);
    }
}
void test_16_direct_ordered_ramp() throws Exception { checkRamp("direct"); }
void test_17_ojp_ordered_ramp_and_mode_isolation() throws Exception { checkRamp("ojp"); }

void test_18_completed_workload_proportions() throws Exception {
    var results = E.runner("it-proportions", "--mode", "both", "--rps", "20", "--warmup", "1", "--duration", "5", "--oltp-percent", "90", "--olap-percent", "10", "--seed", "12345");
    equal(results.stream().map(item -> item.get("mode")).toList(), List.of("direct", "ojp"));
    for (var item : results) {
        absent(item, "error"); var counts = map(at(item, "measure", "counts"));
        equal(counts.get("scheduled"), 100); equal(counts.get("sql_completed"), 100); equal(counts.get("successful_transactions"), 100);
        var operations = map(at(item, "measure", "operations")); equal(at(operations, "report", "sql_completed"), 10);
        equal(operations.entrySet().stream().filter(e -> !e.getKey().equals("report")).mapToDouble(e -> number(at(e.getValue(), "sql_completed"))).sum(), 90);
        assertModeIsolation(item);
        equal(completions(item.get("metrics_after"), item, "measure") - completions(item.get("metrics_before_measure"), item, "measure"), 100);
        equal(completions(item.get("metrics_after"), item, "warmup"), completions(item.get("metrics_before_measure"), item, "warmup"));
    }
}

void test_19_warmup_only_excludes_measurements() throws Exception {
    double before = total(E.metrics(), "demo_completions_total", Map.of("phase", "measure"));
    var results = E.runner("it-warmup-only", "--mode", "direct", "--rps", "2", "--warmup", "1", "--warmup-only");
    equal(results.size(), 1); var item = results.getFirst(); absent(item, "error"); absent(item, "measure");
    equal(at(item, "warmup", "counts", "scheduled"), 2); equal(at(item, "warmup", "counts", "sql_completed"), 2);
    equal(total(E.metrics(), "demo_completions_total", Map.of("phase", "measure")), before);
}

void test_20_independent_open_loop_arrivals() throws Exception {
    String name = E.project + "-controlled-http";
    try {
        E.compose("--profile", "tools", "run", "-d", "--rm", "--no-deps", "--name", name,
            "--env", "TARGET_APP_URL=" + E.env.get("APP_INTERNAL_URL"), "--entrypoint", "java", "load", "/app/DelayProxy.java");
        long end = deadline(60); boolean ready = false;
        while (System.nanoTime() < end) {
            try { if (E.call("/health", null, null, 3, "http://" + name + ":8080", Map.of()).status() == 200) { ready = true; break; } }
            catch (IOException ignored) {}
            sleep(.2);
        }
        require(ready, "Controlled HTTP-delay proxy did not become ready");
        var results = E.runner("it-open-loop", "--url", "http://" + name + ":8080", "--mode", "direct", "--rps", "20", "--warmup", "0", "--duration", "2", "--max-in-flight", "1");
        equal(results.size(), 1); var item = results.getFirst(); absent(item, "error"); var report = map(item.get("measure")); var counts = map(report.get("counts"));
        equal(counts.get("scheduled"), 40, "Arrivals must not wait for preceding responses"); greater(counts.get("dropped"), 20); greater(counts.get("sql_completed"), 0);
        equal(counts.get("sql_completed"), counts.get("dispatched")); equal(counts.get("scheduled"), number(counts.get("dispatched")) + number(counts.get("dropped")));
        equal(counts.get("peak_in_flight"), 1); atLeast(at(report, "latency_ms", "end_to_end", "p50"), 250); require(number(report.get("elapsed_seconds")) < 4, report);
    } finally { E.run(List.of("docker", "rm", "-f", name), 600, false); }
}

void test_21_invalid_cli_parameters() throws Exception {
    var invalid = List.of(List.of("--rps", "0"), List.of("--rps", "-1"), List.of("--rps", "2,2.5"), List.of("--rps", "10001"),
        List.of("--rps", String.join(",", java.util.stream.IntStream.rangeClosed(1, 65).mapToObj(Integer::toString).toList())),
        List.of("--warmup", "-1"), List.of("--duration", "0"), List.of("--max-in-flight", "0"), List.of("--request-timeout", "nan"),
        List.of("--duration", "inf"), List.of("--oltp-percent", "90", "--olap-percent", "20"), List.of("--mode", "invalid"), List.of("--scenario", "invalid"));
    var failures = new ArrayList<String>();
    for (var arguments : invalid) {
        var command = new ArrayList<>(DemoTools.compose(E.env)); command.addAll(List.of("--profile", "tools", "run", "--rm", "--no-deps", "-T", "load")); command.addAll(arguments);
        var result = DemoTools.run(command, E.env, 60);
        E.commands.println("$ " + String.join(" ", command) + "\n" + result.output());
        if (result.exitCode() != 2) failures.add(arguments + ": exit " + result.exitCode() + "\n" + result.output());
    }
    require(failures.isEmpty(), failures);
}

void test_22_htap_entrypoint_verifies_segregation_and_keeps_both_results() throws Exception {
    E.run(List.of(E.script.resolve("htap.sh").toString(), "--rps", "2", "--warmup", "0", "--duration", "5", "--max-in-flight", "32", "--request-timeout", "15", "--run-id", "it-htap"), 300, true);
    var results = E.results("it-htap");
    equal(pairs(results).toString(), List.of(List.of("direct", 2), List.of("ojp", 2)).toString());
    for (var item : results) {
        absent(item, "error"); equal(item.get("scenario"), "htap"); equal(at(item, "measure", "counts", "sql_completed"), 10); require(truth(item.get("benchmark_result_published")), item);
    }
    var classification = map(results.get(1).get("slow_query_classification")); Object observed = classification.get("observed");
    require(observed == null || observed instanceof Boolean, classification);
    if (Boolean.FALSE.equals(observed)) require(classification.get("message").toString().contains("not demonstrated"), classification);
    if (observed == null) require(classification.get("message").toString().contains("could not be verified"), classification);
}

void usage() {
    System.out.println("""
        Usage: integration-test.sh [--keep] [--small] [--only NAME] [--ramp-duration SECONDS]
        Real-container integration tests. Uses only the Java standard library.
          --keep                 Preserve the isolated Compose project
          --small                Explicit 20-session profile (or IT_SMALL=true)
          --only NAME            Match test names; repeat for related scenarios
          --ramp-duration SEC    Seconds per 2,4,8 RPS level (default 10, or IT_RAMP_DURATION)
        """);
}

void main(String[] args) {
    int exit;
    try { exit = execute(args); }
    catch (IllegalArgumentException error) { System.err.println(error.getMessage()); usage(); exit = 2; }
    catch (Exception error) { error.printStackTrace(); exit = 1; }
    if (exit != 0) System.exit(exit);
}

int execute(String[] args) throws Exception {
    for (String arg : args) if (arg.equals("--help") || arg.equals("-h")) { usage(); return 0; }
    var options = new Options();
    for (int i = 0; i < args.length; i++) {
        String option = args[i], value = null;
        int separator = option.indexOf('=');
        if (separator >= 0) {
            value = option.substring(separator + 1);
            option = option.substring(0, separator);
        }
        switch (option) {
            case "--keep" -> {
                if (value != null) throw new IllegalArgumentException("--keep does not accept a value");
                options.keep = true;
            }
            case "--small" -> {
                if (value != null) throw new IllegalArgumentException("--small does not accept a value");
                options.small = true;
            }
            case "--only", "--ramp-duration" -> {
                if (value == null) {
                    if (++i >= args.length) throw new IllegalArgumentException("Missing value for " + option);
                    value = args[i];
                }
                if (option.equals("--only")) options.only.add(value); else options.rampDuration = Double.parseDouble(value);
            }
            default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
        }
    }
    if (!Double.isFinite(options.rampDuration) || options.rampDuration <= 0) throw new IllegalArgumentException("--ramp-duration must be positive and finite");
    E = new Environment(options);
    System.out.println("Project: " + E.project + "; evidence: " + E.output);
    if (options.small) System.out.println("EXPLICIT SMALL PROFILE: 20 direct sessions, not the default 200-session validation.");
    boolean succeeded = false;
    var summary = new ConcurrentHashMap<String, Object>(Map.of("project", E.project, "small_profile", options.small, "passed", false));
    Thread owner = Thread.currentThread();
    Thread shutdown = Thread.ofPlatform().unstarted(() -> {
        E.stopping = true;
        owner.interrupt();
        try { owner.join(5000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        E.finish(summary, false);
    });
    Runtime.getRuntime().addShutdownHook(shutdown);
    try {
        try (var paths = Files.list(E.script)) {
            for (Path script : paths.filter(path -> path.toString().endsWith(".sh") && !path.getFileName().toString().equals("common.sh")).sorted().toList())
                E.run(List.of(script.toString(), "--help"), 20, true);
        }
        E.run(List.of(E.script.resolve("verify-environment.sh").toString()), 60, true);
        E.compose(600, true, "--profile", "tools", "build", "load");
        E.run(List.of(E.script.resolve("start.sh").toString()), 1200, true);
        E.bridge = new DemoTools.Bridge(E.env); E.refreshBase();
        var selected = Arrays.stream(getClass().getDeclaredMethods()).filter(method -> method.getName().startsWith("test_"))
            .sorted(Comparator.comparing(java.lang.reflect.Method::getName))
            .filter(method -> options.only.isEmpty() || options.only.stream().anyMatch(method.getName()::contains)).toList();
        require(!selected.isEmpty(), "--only did not match any integration scenario");
        int failures = 0, errors = 0; long start = System.nanoTime();
        var details = new ArrayList<Object>();
        for (var test : selected) {
            if (E.stopping) break;
            System.out.print(test.getName() + " ... "); System.out.flush();
            long began = System.nanoTime(); var detail = new LinkedHashMap<String, Object>(); detail.put("scenario", test.getName());
            try { test.invoke(this); System.out.println("ok"); detail.put("status", "passed"); }
            catch (java.lang.reflect.InvocationTargetException wrapped) {
                Throwable error = wrapped.getCause();
                if (error instanceof AssertionError) { failures++; detail.put("status", "failure"); System.out.println("FAIL"); }
                else { errors++; detail.put("status", "error"); System.out.println("ERROR"); }
                var trace = new StringWriter(); error.printStackTrace(new PrintWriter(trace));
                System.err.println(test.getName() + "\n" + trace); detail.put("error", error.toString()); detail.put("traceback", trace.toString());
                Files.writeString(E.output.resolve(test.getName() + "-failure.txt"), trace.toString()); E.diagnostics("Integration." + test.getName());
            }
            detail.put("elapsed_seconds", (System.nanoTime() - began) / 1e9); details.add(detail);
        }
        summary.put("tests_run", selected.size()); summary.put("failures", failures); summary.put("errors", errors);
        summary.put("scenarios", selected.stream().map(java.lang.reflect.Method::getName).toList()); summary.put("results", details);
        succeeded = failures == 0 && errors == 0; summary.put("passed", succeeded);
        System.out.printf("Ran %d tests in %.3fs%n%s%n", selected.size(), (System.nanoTime() - start) / 1e9,
            succeeded ? "OK" : "FAILED (failures=" + failures + ", errors=" + errors + ")");
    } catch (Throwable error) {
        error.printStackTrace(); summary.put("startup_error", error.toString()); E.diagnostics("startup-failure");
    } finally {
        E.finish(summary, succeeded);
        try { Runtime.getRuntime().removeShutdownHook(shutdown); } catch (IllegalStateException ignored) {}
    }
    return succeeded ? 0 : 1;
}
