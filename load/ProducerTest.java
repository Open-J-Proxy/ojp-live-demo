import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

// Dependency-free black-box regressions. Run with JDK 25: java /absolute/path/load/ProducerTest.java
final Path root = Path.of(System.getProperty("jdk.launcher.sourcefile")).toAbsolutePath().getParent().getParent();
final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
Path workspace;
Path classes;
int sequence;
final List<String> operations = List.of("browse", "history", "checkout", "review", "account", "report");
final String mix = "browse=30,history=25,checkout=20,review=10,account=5,report=10";
final String reportsOnly = "browse=0,history=0,checkout=0,review=0,account=0,report=100";
final Object childLock = new Object();
final Set<ProcessHandle> children = new HashSet<>();
boolean shuttingDown;

@FunctionalInterface interface Test { void run() throws Exception; }
record Case(String name, Test test) {}
record Result(int code, String out, String err, Path directory) {
    String diagnostic() { return "exit=" + code + "\n" + out + "\n" + err; }
}
record Run(List<Map<String, Object>> reports, Result result) {
    Map<String, Object> report() { return reports.getFirst(); }
}
record Reply(int status, Object body, long delayMillis) {}
record Event(String method, String path, Map<String, String> query, String authorization,
             String contentType, String body, String mode, long received) {}

void main() throws Exception {
    long started = System.nanoTime();
    workspace = Files.createTempDirectory("ojp-producer-tests-");
    classes = Files.createDirectory(workspace.resolve("classes"));
    Thread shutdownHook = new Thread(this::cleanupChildren, "producer-test-child-cleanup");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    int failed = 0;
    try {
        var compilation = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "javac").toString(),
                "--release", "25", "-d", classes.toString()));
        for (String name : List.of("Runner", "LoadJson", "HttpBridge", "DelayProxy"))
            compilation.add(root.resolve("load/" + name + ".java").toString());
        Result compiled = execute(compilation, Map.of(), "", workspace, 90);
        eq(compiled.code(), 0, compiled.diagnostic());
        List<Case> tests = List.of(
                new Case("source_launcher_smoke_and_contract", this::sourceLauncher),
                new Case("strict_cli_validation_precedes_http", this::strictValidation),
                new Case("operation_mix_validation", this::mixValidation),
                new Case("default_profile_empty_warmup", this::defaults),
                new Case("open_loop_bounded_drops_drain_percentiles", this::bounded),
                new Case("202_is_not_sql_completion", this::accepted),
                new Case("readiness_requires_real_sql", this::readinessSql),
                new Case("warmup_only_zero_sql_exportable_failure", this::warmupFailure),
                new Case("ready_flag_precedes_sql_probe", this::readyFlag),
                new Case("control_sequence_warmup_reset_post_only_token", this::controlSequence),
                new Case("request_and_body_deadlines_drain", this::requestTimeout),
                new Case("control_timeout_prevents_load", this::controlTimeout),
                new Case("admission_rollback_invalid_json_contract", this::responseContracts),
                new Case("periodic_cumulative_telemetry_cpu_benchmark", this::telemetry),
                new Case("failed_final_telemetry_preserves_sql", this::telemetryFailure),
                new Case("zero_weights_json_csv_contract", this::csvContract),
                new Case("exact_seeded_blocks_and_sequential_modes", this::seededPlan),
                new Case("negative_arbitrary_precision_seed_vectors", this::seedVectors),
                new Case("all_modes_rates_after_direct_error", this::bothAfterFailure),
                new Case("no_overwrite_json_or_csv_before_http", this::noOverwrite),
                new Case("existing_sampler_directory", this::samplerDirectory),
                new Case("htap_requires_verified_segregation", this::htapRequired),
                new Case("verified_htap_missing_slow_counters", this::missingSlow),
                new Case("unavailable_slow_metrics", this::unavailableSlow),
                new Case("bridge_ids_status_json_body_headers_errors", this::bridge),
                new Case("bridge_stalled_body_recovery_eof_drain", this::bridgeDeadline),
                new Case("proxy_port_selective_delay_forwarding", this::proxy),
                new Case("htap_entrypoint_policy_documentation", this::htapPolicy),
                new Case("htap_rejects_modes_before_docker", this::htapRejected),
                new Case("validate_only_no_http_or_output", this::validateOnly),
                new Case("failover_validation_before_mutation", this::failoverValidation),
                new Case("htap_build_before_sampler_and_load", this::htapRebuild));
        for (Case test : tests) {
            long before = System.nanoTime();
            try {
                test.test().run();
                System.out.printf("PASS %s (%.2fs)%n", test.name(), (System.nanoTime() - before) / 1e9);
            } catch (Throwable error) {
                failed++;
                System.err.println("FAIL " + test.name() + ": " + error);
                error.printStackTrace(System.err);
            }
        }
        System.out.printf("%d passed, %d failed; %d tests in %.2fs%n",
                tests.size() - failed, failed, tests.size(), (System.nanoTime() - started) / 1e9);
    } finally {
        cleanupChildren();
        try { Runtime.getRuntime().removeShutdownHook(shutdownHook); }
        catch (IllegalStateException ignored) {}
        try (var files = Files.walk(workspace)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(file);
        }
    }
    if (failed != 0) System.exit(1);
}

Process startChild(ProcessBuilder builder) throws IOException {
    synchronized (childLock) {
        if (shuttingDown) throw new IOException("Test harness is shutting down");
        Process process = builder.start();
        children.add(process.toHandle());
        return process;
    }
}
void cleanupChildren() {
    List<ProcessHandle> processes;
    synchronized (childLock) {
        shuttingDown = true;
        processes = new ArrayList<>(children);
    }
    var handles = new LinkedHashSet<ProcessHandle>();
    for (ProcessHandle process : processes) {
        process.descendants().forEach(handles::add);
        handles.add(process);
    }
    handles.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroy);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (handles.stream().anyMatch(ProcessHandle::isAlive) && System.nanoTime() < deadline) sleep(20);
    handles.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
}

static void check(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
}
static void eq(Object actual, Object expected) { eq(actual, expected, "values differ"); }
static void eq(Object actual, Object expected, String message) {
    boolean equal = actual instanceof Number a && expected instanceof Number b
            ? new java.math.BigDecimal(a.toString()).compareTo(new java.math.BigDecimal(b.toString())) == 0
            : Objects.equals(actual, expected);
    check(equal, message + ": expected " + expected + ", got " + actual);
}
@SuppressWarnings("unchecked")
static Map<String, Object> object(Object value) { return (Map<String, Object>) value; }
static Object at(Object value, String... path) {
    for (String key : path) value = object(value).get(key);
    return value;
}
static long number(Object value, String... path) { return ((Number) at(value, path)).longValue(); }
static double decimal(Object value, String... path) { return ((Number) at(value, path)).doubleValue(); }
static void near(double a, double b, double tolerance) { check(Math.abs(a - b) <= tolerance, a + " != " + b); }
static Map<String, Object> map(Object... values) {
    var result = new LinkedHashMap<String, Object>();
    for (int i = 0; i < values.length; i += 2) result.put((String) values[i], values[i + 1]);
    return result;
}
static void sleep(long millis) {
    try { Thread.sleep(millis); } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
}

static class Application implements AutoCloseable {
    final HttpServer server;
    final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    final List<Event> events = new CopyOnWriteArrayList<>();
    final AtomicInteger active = new AtomicInteger(), peak = new AtomicInteger(), slow = new AtomicInteger(4);
    final AtomicInteger headersSent = new AtomicInteger();
    volatile String mode = "direct";
    Function<Event, Reply> work, control;
    long arrivalsDelay, bodyDelay;
    boolean slowMetrics = true;
    Application() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(executor);
        server.createContext("/", exchange -> {
            boolean working = false;
            try (exchange) {
                var query = new LinkedHashMap<String, String>();
                String raw = exchange.getRequestURI().getRawQuery();
                if (raw != null) for (String part : raw.split("&")) {
                    String[] pair = part.split("=", 2);
                    query.put(URLDecoder.decode(pair[0], StandardCharsets.UTF_8),
                            URLDecoder.decode(pair.length == 2 ? pair[1] : "", StandardCharsets.UTF_8));
                }
                String path = exchange.getRequestURI().getPath();
                if (path.equals("/mode")) mode = query.get("value");
                Event event = new Event(exchange.getRequestMethod(), path, query,
                        exchange.getRequestHeaders().getFirst("Authorization"),
                        exchange.getRequestHeaders().getFirst("Content-Type"),
                        new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8), mode, System.nanoTime());
                events.add(event);
                Reply reply = new Reply(200, map("success", true), 0);
                switch (path) {
                    case "/health", "/ready" -> reply = new Reply(200, map("ready", true, "status", "up",
                            "active_connections", 2, "direct_sessions", 3, "ojp_sessions", 5,
                            "postgres_snapshot_timestamp_seconds", System.currentTimeMillis() / 1000.0), 0);
                    case "/work" -> {
                        working = true;
                        peak.accumulateAndGet(active.incrementAndGet(), Math::max);
                        if ("report".equals(query.get("op"))) slow.incrementAndGet();
                        reply = work == null ? new Reply(200, map("success", true, "operation", query.get("op")), 0) : work.apply(event);
                    }
                    case "/metrics" -> reply = new Reply(200, "demo_postgres_cpu_percent 12.5\n"
                            + "demo_postgres_resource_timestamp_seconds " + System.currentTimeMillis() / 1000.0 + "\n", 0);
                    case "/ojp/metrics" -> reply = new Reply(200, slowMetrics
                            ? "ojp_sql_slow_executions_total{sql_statement=\"select report\"} " + slow.get() + "\n"
                            + "ojp_sql_slow_executions_total{sql_statement=\"select browse\"} 2\n"
                            : "# no slow-query counters\n", 0);
                    case "/arrivals" -> reply = new Reply(200, map("success", true), arrivalsDelay);
                }
                if (!working && control != null) {
                    Reply override = control.apply(event);
                    if (override != null) reply = override;
                }
                sleep(reply.delayMillis());
                byte[] payload = (reply.body() instanceof String text ? text : LoadJson.stringify(reply.body())).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(reply.status(), payload.length);
                exchange.getResponseBody().flush();
                if (working && "measure".equals(query.get("phase"))) {
                    headersSent.incrementAndGet();
                    sleep(bodyDelay);
                }
                exchange.getResponseBody().write(payload);
            } catch (IOException ignored) {
                // Deadline tests intentionally close the connection before its response.
            } finally {
                if (working) active.decrementAndGet();
            }
        });
        server.start();
    }
    String url() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
    List<Event> requests(String path) { return events.stream().filter(e -> e.path().equals(path)).toList(); }
    List<Event> requests(String path, String phase) {
        return requests(path).stream().filter(e -> phase.equals(e.query().get("phase"))).toList();
    }
    public void close() { server.stop(0); executor.close(); }
}

Result execute(List<String> command, Map<String, String> environment, String input, Path output, int seconds) throws Exception {
    ProcessBuilder builder = new ProcessBuilder(command).directory(root.toFile());
    builder.environment().remove("ADMIN_TOKEN");
    builder.environment().remove("OJP_SLOW_QUERY_SEGREGATION_VERIFIED");
    builder.environment().putAll(environment);
    Process process = startChild(builder);
    try {
        var stdout = CompletableFuture.supplyAsync(() -> read(process.getInputStream()));
        var stderr = CompletableFuture.supplyAsync(() -> read(process.getErrorStream()));
        try (var stdin = process.getOutputStream()) { stdin.write(input.getBytes(StandardCharsets.UTF_8)); }
        check(process.waitFor(seconds, TimeUnit.SECONDS), "child timed out: " + command);
        return new Result(process.exitValue(), stdout.get(5, TimeUnit.SECONDS), stderr.get(5, TimeUnit.SECONDS), output);
    } finally { if (process.isAlive()) process.destroyForcibly().waitFor(); }
}
static String read(InputStream stream) {
    try { return new String(stream.readAllBytes(), StandardCharsets.UTF_8); }
    catch (IOException error) { throw new UncheckedIOException(error); }
}
Path directory() throws IOException { return Files.createDirectory(workspace.resolve("run-" + ++sequence)); }
List<String> child(String name) { return new ArrayList<>(List.of(java, "-cp", classes.toString(), name)); }
Result invoke(List<String> options) throws Exception { return invoke(options, Map.of(), false, directory()); }
Result invoke(List<String> options, Map<String, String> env, boolean source, Path directory) throws Exception {
    List<String> command = source ? new ArrayList<>(List.of(java, root.resolve("load/Runner.java").toString())) : child("Runner");
    command.addAll(List.of("--output", directory.toString()));
    command.addAll(options);
    return execute(command, env, "", directory, 60);
}
Run local(Application app, int expected, Map<String, String> env, boolean source, Path output, String... extra) throws Exception {
    var options = new ArrayList<>(List.of("--url", app.url(), "--ojp-metrics-url", app.url() + "/ojp",
            "--scenario", "integration", "--mode", "direct", "--run-id", "test",
            "--rps", "20", "--warmup", "0", "--duration", ".2", "--request-timeout", "2",
            "--control-timeout", "2", "--readiness-timeout", "2"));
    options.addAll(List.of(extra));
    Result result = invoke(options, env, source, output);
    eq(result.code(), expected, result.diagnostic());
    List<Map<String, Object>> reports = new ArrayList<>();
    try (var files = Files.list(output)) {
        for (Path file : files.filter(p -> p.toString().endsWith(".json")).sorted().toList())
            reports.add(object(LoadJson.parse(Files.readString(file))));
    }
    return new Run(reports, result);
}
Run local(Application app, int expected, String... extra) throws Exception {
    return local(app, expected, Map.of(), false, directory(), extra);
}
static Reply success(Event event, long delay) { return new Reply(200, map("success", true, "operation", event.query().get("op")), delay); }
static boolean measured(Event event) { return "measure".equals(event.query().get("phase")); }

void sourceLauncher() throws Exception {
    try (var app = new Application()) {
        app.control = e -> switch (e.path()) {
            case "/health" -> new Reply(200, map("status", "up"), 0);
            case "/ready" -> new Reply(200, map("healthy", true, "active_connections", 2), 0);
            default -> null;
        };
        Run run = local(app, 0, Map.of(), true, directory());
        eq(run.reports().size(), 1);
        var report = run.report();
        eq(report.get("mode"), "direct"); eq(report.get("rps"), 20); eq(report.get("seed"), 12345);
        eq(at(report, "measure", "counts", "scheduled"), 4);
        eq(at(report, "measure", "telemetry", "final_sent"), true);
        for (String kind : List.of("end_to_end", "service")) {
            var quantiles = object(at(report, "measure", "latency_ms", kind));
            eq(quantiles.get("p95"), quantiles.get("max")); eq(quantiles.get("p99"), quantiles.get("max"));
        }
        for (String key : List.of("counter_semantics", "telemetry_counter_semantics", "benchmark_result_error")) check(report.containsKey(key), key);
        check(app.requests("/benchmark-result").isEmpty(), "no short-run benchmark");
        try (var files = Files.list(run.result().directory())) { check(files.noneMatch(p -> p.toString().endsWith(".partial")), "partial file leaked"); }
        var summary = object(LoadJson.parse(run.result().out().strip().lines().toList().getLast()));
        eq(summary.get("counts"), at(report, "measure", "counts"));
        check(Files.exists(Path.of((String) summary.get("output"))), "summary output missing");
    }
}
void strictValidation() throws Exception {
    var invalid = new ArrayList<>(List.of(
            List.of("--rps", "0"), List.of("--rps", "1,,2"), List.of("--rps", "nan"), List.of("--rps", "inf"),
            List.of("--rps", "2.5"), List.of("--rps", "10001"),
            List.of("--rps", String.join(",", IntStream.rangeClosed(1, 65).mapToObj(Integer::toString).toList())),
            List.of("--duration", "-1"), List.of("--duration", "nan"), List.of("--duration", "0"),
            List.of("--warmup", "-1"), List.of("--warmup", "inf"), List.of("--max-in-flight", "0"),
            List.of("--request-timeout", "0"), List.of("--control-timeout", "0"), List.of("--readiness-timeout", "0"),
            List.of("--oltp-percent", "80"), List.of("--olap-percent", "101"), List.of("--mode", "invalid"),
            List.of("--url", "app:8080"), List.of("--scenario", "arbitrary"), List.of("--mo", "direct"),
            List.of("--warmup-o"), List.of("--scen", "integration"), List.of("--run-id", ""),
            List.of("--run-id", "x".repeat(65)), List.of("--seed", "1.5"), List.of("--max-in-flight", "2.5")));
    try (var app = new Application()) {
        for (var args : invalid) {
            var options = new ArrayList<>(List.of("--url", app.url())); options.addAll(args);
            Result result = invoke(options); eq(result.code(), 2, args + result.diagnostic());
            try (var files = Files.list(result.directory())) { check(files.findAny().isEmpty(), "invalid CLI wrote output"); }
        }
        check(app.events.isEmpty(), "validation made HTTP request");
    }
}
void mixValidation() throws Exception {
    for (String invalid : List.of("browse=90,report=10",
            mix.replace("report=10", "report=nan"), mix.replace("report=10", "report=inf"),
            mix.replace("report=10", "report=10.0"), mix.replace("browse=30", "browse=31"),
            mix.replace("browse=30", "browse=40").replace("account=5", "account=-5"),
            mix.replace("report=10", "other=10"), mix + ",report=10",
            mix.replace("browse=30", "browse=40").replace("report=10", "report=0"))) {
        Result result = invoke(List.of("--operation-mix", invalid)); eq(result.code(), 2, result.diagnostic());
    }
}
void defaults() throws Exception {
    try (var app = new Application()) {
        Result result = invoke(List.of("--url", app.url(), "--ojp-metrics-url", app.url() + "/ojp",
                "--scenario", "integration", "--warmup-only", "--warmup", "0", "--run-id", "defaults"));
        eq(result.code(), 0, result.diagnostic());
        try (var files = Files.list(result.directory())) {
            var reports = files.filter(p -> p.toString().endsWith(".json")).sorted().toList();
            eq(reports.size(), 8);
            for (int i = 0; i < reports.size(); i++) {
                var report = object(LoadJson.parse(Files.readString(reports.get(i))));
                eq(report.get("mode"), i < 4 ? "direct" : "ojp"); eq(report.get("rps"), i % 4 == 1 ? 256 : 8);
                eq(report.get("seed"), 12345); eq(report.get("max_in_flight"), 512);
                eq(report.get("request_timeout"), 30); eq(report.get("control_timeout"), 210);
                var weights = object(report.get("operation_weights"));
                double total = weights.values().stream().mapToDouble(v -> ((Number) v).doubleValue()).sum();
                near(((Number) weights.get("report")).doubleValue() / total, .02, 1e-12);
                near(((Number) weights.get("browse")).doubleValue() / total, .98 / 3, 1e-12);
                check(!report.containsKey("measure") && !report.containsKey("measurement_reset_response"), "unexpected measure");
                eq(at(report, "warmup", "counts", "scheduled"), 0);
                eq(at(report, "warmup", "throughput", "offered_per_second"), 0);
                eq(at(report, "warmup", "latency_ms", "end_to_end", "p99"), null);
                check(report.containsKey("readiness_after"), "readiness missing");
            }
        }
        check(app.requests("/context", "measure").isEmpty(), "measurement context published");
    }
}
void bounded() throws Exception {
    try (var app = new Application()) {
        app.work = e -> success(e, measured(e) ? 180 : 0);
        var phase = at(local(app, 0, "--rps", "100", "--duration", ".35", "--max-in-flight", "2").report(), "measure");
        var counts = at(phase, "counts");
        eq(at(counts, "scheduled"), 35);
        eq(number(counts, "scheduled"), number(counts, "dispatched") + number(counts, "dropped"));
        check(number(counts, "dropped") > 15, "scheduler blocked instead of dropping");
        check(number(counts, "peak_in_flight") <= 2 && app.peak.get() <= 2, "capacity exceeded");
        eq(at(counts, "in_flight"), 0); eq(at(counts, "dispatched"), at(counts, "sql_completed"));
        eq(at(counts, "completed"), at(counts, "dispatched"));
        check(decimal(phase, "elapsed_seconds") < 5, "drain too long");
        check(decimal(phase, "latency_ms", "end_to_end", "p50") > 100, "latency lost");
        check(number(phase, "in_flight_at_arrival_end") > 0 && decimal(phase, "drain_seconds") > 0, "drain not measured");
        eq(object(at(phase, "operations")).values().stream().mapToLong(v -> number(v, "scheduled")).sum(), 35);
        eq(at(phase, "scheduled_latency_histogram", "count"), at(counts, "completed"));
        eq(at(phase, "scheduled_latency_histogram", "buckets", "+Inf"), at(counts, "dispatched"));
        for (String kind : List.of("end_to_end", "service")) {
            double last = 0;
            for (String key : List.of("p50", "p90", "p95", "p99", "max")) {
                double value = decimal(phase, "latency_ms", kind, key); check(value >= last, "unordered quantiles"); last = value;
            }
        }
        String start = (String) at(phase, "start_time"), arrival = (String) at(phase, "end_arrivals_time"), end = (String) at(phase, "end_time");
        check(start.endsWith("Z") && start.compareTo(arrival) <= 0 && arrival.compareTo(end) <= 0, "invalid timestamps");
        near(decimal(phase, "end_arrivals_epoch_seconds") - decimal(phase, "start_epoch_seconds"), .35, 1e-5);
        eq(at(phase, "throughput", "offered_per_second"), 100);
        near(decimal(phase, "throughput", "sql_completions_per_second"),
                number(counts, "sql_completed") / decimal(phase, "elapsed_seconds"), 1e-8);
        near(decimal(phase, "drop_fraction"), number(counts, "dropped") / 35.0, 1e-12);
        check(number(counts, "arrival_window_sql_completed") <= number(counts, "sql_completed"), "arrival count exceeds total");
    }
}
void accepted() throws Exception {
    try (var app = new Application()) {
        app.work = e -> new Reply(measured(e) ? 202 : 200, map("success", true, "operation", e.query().get("op"), "accepted", true), 0);
        var report = local(app, 1).report(); var phase = at(report, "measure"); var counts = at(phase, "counts");
        check(number(counts, "accepted") > 0, "admission missing"); eq(at(counts, "sql_completed"), 0);
        eq(at(counts, "successful_transactions"), 0); eq(at(counts, "dispatched"), at(counts, "failed_transactions"));
        eq(at(phase, "error_fraction"), 1); check(report.get("error").toString().contains("zero SQL"), "zero SQL error missing");
        var last = app.requests("/arrivals", "measure").getLast().query();
        eq(last.get("completed"), "0"); eq(Long.parseLong(last.get("http_completed")), number(counts, "completed"));
        eq(Long.parseLong(last.get("errors")), number(counts, "failed_transactions"));
    }
}
void readinessSql() throws Exception {
    try (var app = new Application()) {
        app.work = e -> new Reply(202, map("success", true, "operation", e.query().get("op")), 0);
        var report = local(app, 1, "--readiness-timeout", ".5").report();
        check(report.get("error").toString().contains("readiness") && !report.containsKey("measure"), "ready without SQL");
        check(!app.requests("/ready").isEmpty() && !app.requests("/work").isEmpty(), "readiness not probed");
        check(app.requests("/work", "measure").isEmpty(), "load started");
    }
}
void warmupFailure() throws Exception {
    try (var app = new Application()) {
        app.work = e -> app.requests("/arrivals", "warmup").isEmpty() ? success(e, 0)
                : new Reply(503, map("success", false, "operation", e.query().get("op")), 0);
        var report = local(app, 1, "--warmup-only", "--warmup", ".1").report();
        check(report.get("error").toString().contains("zero SQL"), "missing warmup failure");
        eq(at(report, "warmup", "counts", "failed_transactions"), 2);
        check(!report.containsKey("measure") && app.requests("/context", "measure").isEmpty(), "measurement started");
    }
}
void readyFlag() throws Exception {
    try (var app = new Application()) {
        app.control = e -> e.path().equals("/ready") ? new Reply(200, map("ready", false), 0) : null;
        var report = local(app, 1, "--readiness-timeout", ".5").report();
        check(report.get("error").toString().contains("readiness"), "missing readiness failure");
        check(!report.containsKey("warmup") && app.requests("/work").isEmpty(), "SQL before ready");
    }
}
void controlSequence() throws Exception {
    try (var app = new Application()) {
        var report = local(app, 0, Map.of("ADMIN_TOKEN", "fixture-only"), false, directory(), "--warmup", ".1").report();
        eq(at(report, "warmup", "counts", "scheduled"), 2); eq(at(report, "measure", "counts", "scheduled"), 4);
        eq(at(report, "measure", "counts", "sql_completed"), 4);
        eq(at(report, "readiness_after", "response", "active_connections"), 2);
        Event first = app.events.getFirst(); eq(first.method(), "POST"); eq(first.path(), "/mode");
        eq(first.query(), Map.of("value", "direct"));
        Event context = app.requests("/context", "measure").getFirst();
        int index = app.events.indexOf(context);
        eq(app.events.subList(index - 1, index + 4).stream().map(Event::path).toList(),
                List.of("/reset", "/context", "/metrics", "/arrivals", "/work"));
        for (Event event : app.events) eq(event.authorization(), event.method().equals("POST") ? "Bearer " + "fixture-only" : null);
        for (String phase : List.of("warmup", "measure")) {
            var publications = app.requests("/arrivals", phase);
            var initial = publications.getFirst().query(); var last = publications.getLast().query();
            eq(initial.get("reset"), "true"); eq(initial.get("scheduled"), "0"); eq(initial.get("completed"), "0");
            eq(last.get("reset"), "false"); eq(last.get("final"), "true"); eq(last.get("inflight"), "0");
            eq(Long.parseLong(last.get("latency_count")), number(report, phase, "counts", "completed"));
            var buckets = Arrays.stream(last.get("latency_bucket_counts").split(",")).map(Long::parseLong).toList();
            eq(buckets.size(), 14); eq(buckets, buckets.stream().sorted().toList());
            eq(buckets.getLast(), Long.parseLong(last.get("latency_count")));
        }
        check(app.events.indexOf(app.requests("/arrivals", "warmup").getLast()) < index, "warmup did not drain");
    }
}
void requestTimeout() throws Exception {
    for (boolean stalledBody : List.of(false, true)) try (var app = new Application()) {
        if (stalledBody) app.bodyDelay = 600;
        app.work = e -> success(e, measured(e) && !stalledBody ? 600 : 0);
        var counts = at(local(app, 1, "--request-timeout", ".15", "--rps", "10", "--duration", ".2").report(), "measure", "counts");
        eq(at(counts, "timeouts"), 2); eq(at(counts, "completed"), 2);
        eq(at(counts, "sql_completed"), 0); eq(at(counts, "in_flight"), 0);
        if (stalledBody) check(app.headersSent.get() > 0, "body deadline not exercised");
    }
}
void controlTimeout() throws Exception {
    try (var app = new Application()) {
        app.control = e -> e.path().equals("/mode") ? new Reply(200, map("success", true), 600) : null;
        var report = local(app, 1, "--control-timeout", ".15").report();
        check(report.containsKey("error") && !report.containsKey("warmup") && app.requests("/work").isEmpty(), "load after control timeout");
    }
}
void responseContracts() throws Exception {
    try (var app = new Application()) {
        AtomicInteger index = new AtomicInteger();
        List<Reply> replies = List.of(new Reply(200, map("success", false, "accepted", false), 0),
                new Reply(500, map("success", false, "accepted", true), 0),
                new Reply(200, map("success", true, "sql_completed", 0), 0),
                new Reply(200, map("success", true, "rollback", true), 0),
                new Reply(200, map("success", true, "operation", "unknown"), 0),
                new Reply(200, "not json", 0), new Reply(200, map("success", true), 0));
        app.work = e -> {
            if (!measured(e)) return new Reply(200, map("success", true, "op", e.query().get("op")), 0);
            Reply reply = replies.get(index.getAndIncrement());
            if (reply.body() instanceof Map<?, ?> body) {
                var payload = map("operation", e.query().get("op")); payload.putAll(object(body));
                return new Reply(reply.status(), payload, 0);
            }
            return reply;
        };
        var report = local(app, 0, "--rps", "10", "--duration", ".7").report();
        check(!report.containsKey("error"), "transient errors failed whole run");
        var phase = at(report, "measure");
        eq(at(phase, "counts", "accepted"), 6); eq(at(phase, "counts", "sql_completed"), 2);
        eq(at(phase, "counts", "successful_transactions"), 1); eq(at(phase, "counts", "failed_transactions"), 6);
        for (String key : List.of("sql_failed", "http_500", "sql_not_completed", "rollback", "operation_mismatch", "invalid_json"))
            eq(at(phase, "errors", key), 1);
        eq(object(at(phase, "errors")).size(), 6);
    }
}
void telemetry() throws Exception {
    try (var app = new Application()) {
        app.arrivalsDelay = 150;
        var report = local(app, 0, "--rps", "40", "--duration", "2.4").report();
        var phase = at(report, "measure");
        eq(at(phase, "counts", "scheduled"), 96); eq(at(phase, "counts", "sql_completed"), 96);
        check(decimal(phase, "elapsed_seconds") < 8, "telemetry blocked scheduler");
        var publications = app.requests("/arrivals", "measure"); check(publications.size() >= 3, "periodic telemetry missing");
        for (String key : List.of("scheduled", "dispatched", "completed", "errors", "http_completed")) {
            var values = publications.stream().map(e -> Long.parseLong(e.query().get(key))).toList();
            eq(values, values.stream().sorted().toList());
        }
        eq(publications.getFirst().query().get("reset"), "true");
        check(publications.subList(1, publications.size()).stream().allMatch(e -> e.query().get("reset").equals("false")), "multiple resets");
        eq(publications.getLast().query().get("scheduled"), "96"); eq(publications.getLast().query().get("final"), "true");
        for (String key : List.of("postgres_cpu_samples", "postgres_connection_samples")) {
            var values = (List<?>) at(phase, key); check(!values.isEmpty(), key);
            for (Object value : values) eq(value, key.contains("cpu") ? 12.5 : 3);
        }
        for (String key : List.of("postgres_cpu_sample_times", "postgres_connection_sample_times"))
            for (Object value : (List<?>) at(phase, key)) {
                double time = ((Number) value).doubleValue();
                check(time >= decimal(phase, "start_epoch_seconds") && time <= decimal(phase, "end_arrivals_epoch_seconds"), "sample outside measurement");
            }
        var benchmark = app.requests("/benchmark-result").getLast().query();
        eq(Double.parseDouble(benchmark.get("physical_connections")), 3);
        eq(Double.parseDouble(benchmark.get("postgres_cpu_percent")), 12.5); eq(benchmark.get("mode"), "direct");
        eq(at(report, "benchmark_result_published", "success"), true);
    }
}
void telemetryFailure() throws Exception {
    try (var app = new Application()) {
        app.control = e -> e.path().equals("/arrivals") ? new Reply(503, map("success", false), 0) : null;
        Run run = local(app, 0);
        eq(at(run.report(), "measure", "telemetry", "final_sent"), false);
        check(run.result().err().contains("WARNING: final /arrivals publication failed"), "warning missing");
        check(number(run.report(), "measure", "counts", "sql_completed") > 0, "SQL results lost");
    }
}

List<Map<String, String>> csv(Path directory) throws Exception {
    Path file;
    try (var files = Files.list(directory)) { file = files.filter(p -> p.toString().endsWith(".csv")).sorted().findFirst().orElseThrow(); }
    String text = Files.readString(file);
    List<List<String>> records = new ArrayList<>();
    List<String> row = new ArrayList<>();
    StringBuilder field = new StringBuilder();
    boolean quoted = false;
    for (int i = 0; i < text.length(); i++) {
        char c = text.charAt(i);
        if (c == '"') {
            if (quoted && i + 1 < text.length() && text.charAt(i + 1) == '"') { field.append('"'); i++; }
            else quoted = !quoted;
        } else if (!quoted && (c == ',' || c == '\n')) {
            row.add(field.toString()); field.setLength(0);
            if (c == '\n') { records.add(row); row = new ArrayList<>(); }
        } else if (c != '\r' || quoted) field.append(c);
    }
    if (!row.isEmpty() || !field.isEmpty()) { row.add(field.toString()); records.add(row); }
    check(!quoted, "unterminated CSV quote");
    var header = records.removeFirst();
    var result = new ArrayList<Map<String, String>>();
    for (var record : records) {
        eq(record.size(), header.size());
        var values = new LinkedHashMap<String, String>();
        for (int i = 0; i < header.size(); i++) values.put(header.get(i), record.get(i));
        result.add(values);
    }
    return result;
}
void csvContract() throws Exception {
    try (var app = new Application()) {
        Run run = local(app, 0, "--operation-mix", reportsOnly, "--oltp-percent", "0", "--olap-percent", "100", "--warmup-only", "--warmup", ".1");
        var report = run.report(); eq(report.get("warmup_only"), true); check(!report.containsKey("measure"), "measure in warmup-only");
        check(app.requests("/work").stream().allMatch(e -> e.query().get("op").equals("report")), "zero-weight op selected");
        for (String op : operations) eq(at(report, "operation_weights", op), op.equals("report") ? 100 : 0);
        var rows = csv(run.result().directory()); eq(rows.size(), 7);
        var byOperation = new HashMap<String, Map<String, String>>(); rows.forEach(r -> byOperation.put(r.get("operation"), r));
        eq(byOperation.keySet(), Set.of("all", "browse", "history", "checkout", "review", "account", "report"));
        eq(Double.parseDouble(byOperation.get("all").get("offered_per_second")), 20);
        check(Double.parseDouble(byOperation.get("all").get("successful_completions_per_second")) > 0, "throughput missing");
        eq(byOperation.get("browse").get("scheduled"), "0");
        eq(at(report, "warmup", "operations", "browse", "latency_ms", "service", "p50"), null);
        var aggregate = new ArrayList<>(object(at(report, "warmup", "throughput")).keySet());
        aggregate.addAll(List.of("error_fraction", "drop_fraction"));
        for (String op : operations) {
            var row = byOperation.get(op);
            for (String key : aggregate) eq(row.get(key), "");
            eq(LoadJson.parse(row.get("errors_json")), at(report, "warmup", "errors"));
            eq(LoadJson.parse(row.get("readiness_after_json")), report.get("readiness_after"));
        }
    }
}

void seededPlan() throws Exception {
    try (var app = new Application()) {
        Run run = local(app, 0, "--mode", "both", "--rps", "100", "--duration", "1",
                "--operation-mix", mix, "--oltp-percent", "90", "--olap-percent", "10");
        eq(run.reports().stream().map(r -> r.get("mode")).toList(), List.of("direct", "ojp"));
        List<String> prior = null;
        for (String mode : List.of("direct", "ojp")) {
            var work = app.requests("/work", "measure").stream().filter(e -> e.mode().equals(mode)).toList();
            eq(work.size(), 100);
            int[] weights = {30, 25, 20, 10, 5, 10};
            for (int i = 0; i < operations.size(); i++) {
                String op = operations.get(i); eq(work.stream().filter(e -> op.equals(e.query().get("op"))).count(), weights[i]);
            }
            List<String> plan = work.stream().map(e -> e.query().get("seed") + ":" + e.query().get("op")).sorted().toList();
            if (prior != null) eq(plan, prior);
            prior = plan;
            // Fixed CPython integer-seeded MT19937 shuffle/getrandbits fixture, not a second call to production RNG.
            eq(hex(MessageDigest.getInstance("SHA-256").digest(String.join("\n", plan).getBytes(StandardCharsets.UTF_8))),
                    PLAN_SHA256);
        }
        eq(app.requests("/mode").stream().map(e -> e.query().get("value")).toList(), List.of("direct", "ojp"));
        var classification = at(run.reports().get(1), "slow_query_classification");
        eq(at(classification, "observed"), true); eq(at(classification, "count"), 10);
        eq(at(classification, "by_statement", "select report"), 10); eq(at(classification, "by_statement", "select browse"), 0);
    }
}
static String hex(byte[] bytes) { return HexFormat.of().formatHex(bytes); }
final String PLAN_SHA256 = "beaaeee66e3486803ee1cd94344af480176c337389a541f90fb06d3430424721";
void seedVectors() throws Exception {
    // CPython random.Random(integer).getrandbits(63), including abs(seed) and all big-integer words.
    String[][] fixtures = {
            {"-12345", "6757828938340735367", "7564776570512634437"},
            {"1606938044258990275541962092341162602522202993782792835301393", "5810804579299733539", "6975881490232287375"}
    };
    for (String[] fixture : fixtures) try (var app = new Application()) {
        var report = local(app, 0, "--seed", fixture[0], "--operation-mix", reportsOnly,
                "--oltp-percent", "0", "--olap-percent", "100", "--duration", ".1").report();
        eq(new BigInteger(report.get("seed").toString()), new BigInteger(fixture[0]));
        eq(app.requests("/work", "measure").stream().map(e -> e.query().get("seed")).sorted().toList(),
                List.of(fixture[1], fixture[2]).stream().sorted().toList());
    }
}
void bothAfterFailure() throws Exception {
    try (var app = new Application()) {
        app.control = e -> e.path().equals("/mode") && e.mode().equals("direct") ? new Reply(503, map("success", false), 0) : null;
        Run run = local(app, 1, "--mode", "both", "--rps", "30,10,30", "--duration", ".1");
        eq(run.reports().size(), 6);
        for (int i = 0; i < 6; i++) {
            var report = run.reports().get(i); eq(report.get("mode"), i < 3 ? "direct" : "ojp"); eq(report.get("rps"), i % 3 == 1 ? 10 : 30);
            if (i < 3) check(report.containsKey("error"), "failure omitted");
            else check(number(report, "measure", "counts", "sql_completed") > 0, "later mode not attempted");
        }
        eq(app.requests("/mode").stream().map(e -> e.query().get("value")).toList(), List.of("direct", "direct", "direct", "ojp", "ojp", "ojp"));
        var rows = csv(run.result().directory()); eq(rows.size(), 1); eq(rows.getFirst().get("error"), run.report().get("error"));
    }
}
void noOverwrite() throws Exception {
    Path directory = directory();
    for (String extension : List.of("json", "csv")) try (var app = new Application()) {
        Path existing = directory.resolve("reused_run-001-direct-1." + extension); Files.writeString(existing, "do not overwrite");
        Result result = invoke(List.of("--url", app.url(), "--mode", "direct", "--run-id", "reused/run"), Map.of(), false, directory);
        eq(result.code(), 2, result.diagnostic()); check(result.err().contains("choose a unique --run-id"), "overwrite diagnostic missing");
        eq(Files.readString(existing), "do not overwrite"); check(app.events.isEmpty(), "HTTP before collision check"); Files.delete(existing);
    }
}
void samplerDirectory() throws Exception {
    Path output = directory(); Files.createDirectory(output.resolve("test"));
    try (var app = new Application()) { eq(local(app, 0, Map.of(), false, output).reports().size(), 1); }
}
void htapRequired() throws Exception {
    try (var app = new Application()) {
        Result result = invoke(List.of("--url", app.url(), "--rps", "1")); eq(result.code(), 2, result.diagnostic());
        check(result.err().contains("requires scripts/htap.sh") && app.events.isEmpty(), "HTAP guard missing");
        try (var files = Files.list(result.directory())) { check(files.findAny().isEmpty(), "guard wrote output"); }
    }
}
void missingSlow() throws Exception {
    try (var app = new Application()) {
        app.slowMetrics = false;
        var classification = at(local(app, 0, Map.of("OJP_SLOW_QUERY_SEGREGATION_VERIFIED", "true"), false,
                directory(), "--mode", "ojp", "--scenario", "htap").report(), "slow_query_classification");
        eq(at(classification, "observed"), false); eq(at(classification, "count"), 0);
        check(at(classification, "message").toString().contains("not demonstrated"), "unsupported classification");
    }
}
void unavailableSlow() throws Exception {
    try (var app = new Application()) {
        app.control = e -> e.path().equals("/ojp/metrics") ? new Reply(503, "unavailable", 0) : null;
        var classification = at(local(app, 0, "--mode", "ojp").report(), "slow_query_classification");
        eq(at(classification, "observed"), null); eq(at(classification, "count"), null);
        check(at(classification, "message").toString().contains("unavailable"), "unavailable not reported");
    }
}

void bridge() throws Exception {
    try (var app = new Application()) {
        app.control = e -> switch (e.path()) {
            case "/echo" -> new Reply(201, map("echo", "snowman ☃\nquoted \"value\""), 0);
            case "/unavailable" -> new Reply(503, "upstream unavailable", 0);
            default -> null;
        };
        var payload = map("nested", Arrays.asList(null, true, 3, "snowman ☃"));
        List<Map<String, Object>> messages = List.of(map("id", "ready", "url", app.url() + "/ready", "timeout", 5),
                map("id", 42, "url", app.url() + "/echo?text=a%26b", "timeout", 5, "method", "POST",
                        "data", payload, "headers", map("Authorization", "Bearer " + "fixture-only", "Content-Type", "application/json")),
                map("id", "http-error", "url", app.url() + "/unavailable", "timeout", 5),
                map("id", "invalid-url", "url", "http://[broken?private-marker", "timeout", 5));
        Result result = execute(child("HttpBridge"), Map.of(), String.join("\n", messages.stream().map(LoadJson::stringify).toList()) + "\n", workspace, 30);
        eq(result.code(), 0, result.diagnostic());
        var replies = new HashMap<String, Map<String, Object>>();
        result.out().lines().map(LoadJson::parse).map(r -> object(r)).forEach(r -> replies.put(r.get("id").toString(), r));
        eq(replies.keySet(), Set.of("ready", "42", "http-error", "invalid-url"));
        eq(replies.get("ready").get("status"), 200);
        eq(at(LoadJson.parse(replies.get("ready").get("body").toString()), "ready"), true);
        eq(replies.get("42").get("status"), 201);
        eq(LoadJson.parse(replies.get("42").get("body").toString()), map("echo", "snowman ☃\nquoted \"value\""));
        eq(replies.get("http-error").get("status"), 503); eq(replies.get("http-error").get("body"), "upstream unavailable");
        var invalid = replies.get("invalid-url");
        check(invalid.containsKey("error") && !invalid.containsKey("status") && !invalid.get("error").toString().contains("private-marker"), "unsafe bridge error");
        Event posted = app.requests("/echo").getFirst();
        eq(posted.query(), Map.of("text", "a&b")); eq(LoadJson.parse(posted.body()), LoadJson.parse(LoadJson.stringify(payload)));
        eq(posted.authorization(), "Bearer " + "fixture-only"); eq(posted.contentType(), "application/json");
        eq(app.requests("/ready").getFirst().authorization(), null);
    }
}
Map<String, Object> exchange(BufferedWriter input, BufferedReader output, Map<String, Object> message) throws Exception {
    input.write(LoadJson.stringify(message)); input.newLine(); input.flush();
    return CompletableFuture.supplyAsync(() -> {
        try { return object(LoadJson.parse(output.readLine())); } catch (IOException e) { throw new UncheckedIOException(e); }
    }).get(15, TimeUnit.SECONDS);
}
void bridgeDeadline() throws Exception {
    try (var app = new Application()) {
        app.bodyDelay = 700;
        Process process = startChild(new ProcessBuilder(child("HttpBridge")));
        var stderr = CompletableFuture.supplyAsync(() -> read(process.getErrorStream()));
        try (var input = process.outputWriter(StandardCharsets.UTF_8); var output = process.inputReader(StandardCharsets.UTF_8)) {
            eq(exchange(input, output, map("id", "warmup", "url", app.url() + "/ready", "timeout", 5)).get("status"), 200);
            var timeout = exchange(input, output, map("id", "body-timeout", "url", app.url() + "/work?op=browse&phase=measure", "timeout", .2));
            eq(timeout.get("id"), "body-timeout"); check(timeout.containsKey("error"), "body deadline missing");
            check(app.headersSent.get() > 0, "no headers before timeout");
            var recovered = exchange(input, output, map("id", "recovered", "url", app.url() + "/ready", "timeout", 5));
            eq(recovered.get("id"), "recovered"); eq(recovered.get("status"), 200);
            // EOF must drain an outstanding request, not simply abandon virtual threads.
            input.write(LoadJson.stringify(map("id", "eof-drain", "url", app.url() + "/ready", "timeout", 5)));
            input.newLine(); input.flush(); process.getOutputStream().close();
            check(process.waitFor(10, TimeUnit.SECONDS), "bridge did not exit at EOF");
            var drained = object(LoadJson.parse(output.readLine())); eq(drained.get("id"), "eof-drain"); eq(drained.get("status"), 200);
            eq(output.readLine(), null); eq(process.exitValue(), 0, stderr.get());
        } finally { if (process.isAlive()) process.destroyForcibly().waitFor(); }
    }
}
HttpResponse<String> get(HttpClient client, String url) throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
}
void proxy() throws Exception {
    try (var app = new Application(); var client = HttpClient.newHttpClient()) {
        app.control = e -> switch (e.path()) {
            case "/echo" -> new Reply(201, map("echo", e.body()), 0);
            case "/unavailable" -> new Reply(503, "upstream unavailable", 0);
            default -> null;
        };
        int port;
        try (var reservation = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) { port = reservation.getLocalPort(); }
        var builder = new ProcessBuilder(child("DelayProxy"));
        builder.environment().putAll(Map.of("TARGET_APP_URL", app.url(), "PORT", Integer.toString(port)));
        Process process = startChild(builder);
        var stderr = CompletableFuture.supplyAsync(() -> read(process.getErrorStream()));
        try {
            String base = "http://127.0.0.1:" + port;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (true) {
                check(process.isAlive(), "proxy exited");
                try { eq(at(LoadJson.parse(get(client, base + "/ready").body()), "ready"), true); break; }
                catch (IOException error) { check(System.nanoTime() < deadline, "proxy readiness timeout"); sleep(50); }
            }
            long started = System.nanoTime();
            var response = get(client, base + "/work?op=report&phase=measure&seed=7");
            eq(response.statusCode(), 200); eq(at(LoadJson.parse(response.body()), "operation"), "report");
            Event forwarded = app.requests("/work").getFirst();
            check(forwarded.received() - started >= TimeUnit.MILLISECONDS.toNanos(200), "work delay missing");
            eq(forwarded.query(), Map.of("op", "report", "phase", "measure", "seed", "7"));
            String payload = "{\"message\":\"forwarded\"}";
            started = System.nanoTime();
            var post = HttpRequest.newBuilder(URI.create(base + "/echo?label=a%26b"))
                    .header("Authorization", "Bearer " + "fixture-only").header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload)).build();
            response = client.send(post, HttpResponse.BodyHandlers.ofString());
            eq(response.statusCode(), 201); eq(at(LoadJson.parse(response.body()), "echo"), payload);
            Event posted = app.requests("/echo").getFirst();
            // Compare arrival at upstream, not response latency; cold JIT and scheduling are deliberately excluded.
            check(posted.received() >= started, "invalid proxy timestamp");
            eq(posted.method(), "POST"); eq(posted.query(), Map.of("label", "a&b"));
            eq(posted.authorization(), "Bearer " + "fixture-only"); eq(posted.contentType(), "application/json");
            response = get(client, base + "/unavailable");
            eq(response.statusCode(), 503); eq(response.body(), "upstream unavailable");
        } finally {
            process.destroy(); if (!process.waitFor(10, TimeUnit.SECONDS)) process.destroyForcibly().waitFor();
            check(stderr.get().isBlank(), "proxy stderr: " + stderr.get());
        }
    }
}

void htapPolicy() throws Exception {
    String script = Files.readString(root.resolve("scripts/htap.sh")), readme = Files.readString(root.resolve("README.md"));
    for (String required : List.of("export SLOW_QUERY_ENABLED=true", "export OJP_SLOW_QUERY_SEGREGATION_VERIFIED=true",
            "-Dojp.server.slowQuerySegregation.enabled=true", "-Dojp.server.slowQuerySegregation.slowSlotPercentage=20",
            "--rps 8,256,8,8 --warmup 10 --duration 30", "--oltp-percent 98 --olap-percent 2",
            "java \"$ROOT_DIR/load/Resources.java\" \"$@\"")) check(script.contains(required), "HTAP policy missing: " + required);
    for (String required : List.of("```mermaid", "Docker Compose", "PostgreSQL", "OJP server", "Prometheus",
            "Grafana", "postgres-exporter", "Host resource sampler", "Java open-loop load runner")) check(readme.contains(required), required);
    for (String old : List.of("death-spiral.sh", "slow-queries.sh", "throttling.sh", "compare.sh", "failover.sh", "warmup.sh"))
        check(!Files.exists(root.resolve("scripts/" + old)) && !readme.contains(old), "obsolete entrypoint " + old);
}
void htapRejected() throws Exception {
    for (List<String> options : List.of(List.of("--scenario", "failover"), List.of("--scenario=integration"),
            List.of("--mode", "direct"), List.of("--mode=ojp"), List.of("--warmup-only"))) {
        var command = new ArrayList<>(List.of("bash", root.resolve("scripts/htap.sh").toString())); command.addAll(options);
        Result result = execute(command, Map.of(), "", workspace, 10);
        eq(result.code(), 2, result.diagnostic()); check(result.err().contains("HTAP always measures Direct JDBC then OJP"), "wrong policy rejection");
    }
}
void validateOnly() throws Exception {
    try (var app = new Application()) {
        Result result = invoke(List.of("--validate-only", "--url", app.url(), "--mode", "ojp", "--rps", "10"));
        eq(result.code(), 0, result.diagnostic());
        try (var files = Files.list(result.directory())) { check(files.findAny().isEmpty(), "validate-only wrote output"); }
        for (List<String> options : List.of(List.of("--rps", "0"), List.of("--duration", "nan"),
                List.of("--operation-mix", "report=100"), List.of("--validate-only=true"))) {
            var args = new ArrayList<>(List.of("--validate-only", "--url", app.url())); args.addAll(options);
            result = invoke(args); eq(result.code(), 2, result.diagnostic());
        }
        check(app.events.isEmpty(), "validation used HTTP");
    }
}
void failoverValidation() throws Exception {
    Path source = root.resolve("load/Failover.java");
    check(Files.exists(source), "Failover.java missing");
    String text = Files.readString(source);
    check(text.contains("--validate-only") && text.contains("--scenario") && text.contains("failover"), "failover validation contract missing");
    for (String flag : List.of("--warmup-only", "--validate-only")) {
        Result result = execute(List.of(java, source.toString(), flag), Map.of("RESULTS_DIR", directory().toString()), "", workspace, 45);
        check(result.code() != 0 && result.err().contains("not supported"), result.diagnostic());
    }
    hostValidation(source);
}
void hostValidation(Path source) throws Exception {
    Path bin = Files.createDirectory(directory().resolve("bin"));
    Path log = bin.resolve("commands");
    Path docker = bin.resolve("docker");
    Files.writeString(docker, "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$TEST_COMMAND_LOG\"\nexit 2\n");
    check(docker.toFile().setExecutable(true), "cannot create docker fixture");
    Path output = directory(), override = output.resolve("compose.override.json");
    Files.writeString(override, "{}");
    var environment = Map.of("PATH", bin + File.pathSeparator + System.getenv("PATH"), "TEST_COMMAND_LOG", log.toString(),
            "RESULTS_DIR", output.toString(), "COMPOSE_OVERRIDE_FILE", override.toString());
    for (List<String> invalid : List.of(List.of("--rps", "0"), List.of("--duration", "nan"),
            List.of("--operation-mix", "report=100"), List.of("--mode", "direct"))) {
        var arguments = new ArrayList<>(List.of(java, source.toString())); arguments.addAll(invalid);
        Result result = execute(arguments, environment, "", output, 45);
        check(result.code() != 0, "invalid failover CLI accepted: " + invalid);
        check(!Files.exists(log), "Docker used before failover validation: " + invalid);
    }
    String text = Files.readString(source);
    int validation = text.indexOf("Runner.arguments(");
    check(validation >= 0 && validation < text.indexOf("new Resources.ResourceSampler")
            && validation < text.indexOf("\"up\""), "failover must validate before sampler and service mutation");
    // Valid arguments must progress past validation and retain Compose override configuration.
    Result result = execute(List.of(java, source.toString(), "--run-id", "test-ha", "--rps", "10", "--warmup", "1", "--duration", "8"),
            environment, "", output, 45);
    check(result.code() != 0, "Docker failure ignored");
    var commands = Files.readAllLines(log);
    String command = commands.getFirst();
    check(command.contains("-f " + override) && command.contains("--profile ha up"), "wrong Compose override: " + command);
}
void htapRebuild() throws Exception {
    Path source = root.resolve("load/Resources.java");
    check(Files.exists(source), "Resources.java missing");
    Path bin = Files.createDirectory(directory().resolve("bin")), log = bin.resolve("commands");
    Path docker = bin.resolve("docker");
    Files.writeString(docker, "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$TEST_COMMAND_LOG\"\nexit 7\n");
    check(docker.toFile().setExecutable(true), "cannot create docker fixture");
    Path output = directory();
    Result result = execute(List.of(java, source.toString(), "--run-id", "test-htap", "--rps", "2"),
            Map.of("PATH", bin + File.pathSeparator + System.getenv("PATH"), "TEST_COMMAND_LOG", log.toString(),
                    "RESULTS_DIR", output.toString()), "", output, 45);
    check(result.code() != 0, "failed build was ignored");
    var commands = Files.readAllLines(log); eq(commands.size(), 1);
    check(commands.getFirst().endsWith("--profile tools build load"), "load was not rebuilt first: " + commands);
    try (var files = Files.walk(output)) {
        check(files.noneMatch(p -> p.toString().endsWith("resources.jsonl")), "sampler started after failed build");
    }
    Files.writeString(docker, "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$TEST_COMMAND_LOG\"\ncase \" $* \" in\n"
            + "  *' run '*) exit 7;;\n  *) exit 0;;\nesac\n");
    Files.delete(log);
    result = execute(List.of(java, source.toString(), "--run-id", "test-htap", "--rps", "2"),
            Map.of("PATH", bin + File.pathSeparator + System.getenv("PATH"), "TEST_COMMAND_LOG", log.toString(),
                    "RESULTS_DIR", output.toString()), "", output, 45);
    eq(result.code(), 7, result.diagnostic());
    commands = Files.readAllLines(log);
    check(commands.getFirst().endsWith("--profile tools build load"), "build not first");
    String load = commands.stream().filter(c -> c.contains(" run ")).findFirst().orElseThrow();
    check(load.contains("--rps 2") && load.endsWith("--scenario htap --mode both"), "wrong HTAP load options: " + load);
    check(Files.exists(output.resolve("test-htap/resources.jsonl")), "sampler never created evidence");
    String text = Files.readString(source);
    int build = text.indexOf("\"build\""), start = text.indexOf("sampler.start()", build), run = text.indexOf("new ProcessBuilder", build);
    check(build >= 0 && start > build && run > start && text.indexOf("cleanup.run()", run) > run
                    && text.contains("sampler.stop()"),
            "expected build, sampler start, load run, sampler stop ordering");
    check(text.contains("\"htap\"") && text.contains("\"both\""), "HTAP mode/scenario missing");
}
