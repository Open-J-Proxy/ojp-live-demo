package demo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class Main implements AutoCloseable {
    private final Config config;
    private final Metrics metrics = new Metrics();
    private final Database database;
    private final HttpServer workServer, adminServer;
    private final ThreadPoolExecutor workers, adminWorkers;
    private final Semaphore admission, mutation = new Semaphore(1);
    private final AtomicInteger inflight = new AtomicInteger(), holdActive = new AtomicInteger(), holdPeak = new AtomicInteger();
    private volatile String runId = "";

    Main(Config config) throws IOException {
        this.config = config;
        database = new Database(config, metrics);
        admission = new Semaphore(config.workers());
        workers = executor(config.workers(), config.queueSize(), "http-worker");
        adminWorkers = executor(16, 128, "http-admin");
        workServer = HttpServer.create(new InetSocketAddress(config.bind(), config.port()), config.queueSize());
        adminServer = HttpServer.create(new InetSocketAddress(config.bind(), config.adminPort()), 128);
        workServer.setExecutor(workers); adminServer.setExecutor(adminWorkers);
        workServer.createContext("/", this::handle);
        adminServer.createContext("/", this::handleAdmin);
    }

    private static ThreadPoolExecutor executor(int size, int queue, String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return new ThreadPoolExecutor(size, size, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queue), r -> new Thread(r, prefix + "-" + sequence.incrementAndGet()),
                new ThreadPoolExecutor.AbortPolicy());
    }

    void start(boolean initialize) {
        workServer.start(); adminServer.start();
        if (System.getenv().getOrDefault("ADMIN_TOKEN", "").isBlank())
            System.err.println("WARNING: unauthenticated demo admin endpoints; publish ports ONLY on loopback/trusted networks.");
        System.out.println("demo HTTP=" + config.port() + " admin/metrics=" + config.adminPort()
                + " workers=" + config.workers() + " queue=" + config.queueSize());
        if (initialize) Thread.ofPlatform().name("database-startup").start(database::initialize);
    }

    private void handleAdmin(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (Set.of("/work", "/hold").contains(path)) {
            send(exchange, 404, Map.of("error", "Work routes are only available on PORT")); return;
        }
        handle(exchange);
    }

    private void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        try {
            Map<String, String> query = query(exchange.getRequestURI().getRawQuery());
            switch (path) {
                case "/health" -> {
                    method(exchange, "GET");
                    Metrics.Context context = metrics.context;
                    Map<String, Object> metadata = Map.of("phase", context.phase(), "scenario", context.scenario(),
                            "level", context.level(), "run_id", runId);
                    send(exchange, 200, Map.of("status", "up", "mode", database.mode, "ready", database.ready,
                            "phase", context.phase(), "scenario", context.scenario(), "level", context.level(),
                            "run_id", runId, "context", metadata));
                }
                case "/ready" -> {
                    method(exchange, "GET");
                    Map<String, Object> diagnostic = database.diagnostics(true);
                    send(exchange, Boolean.TRUE.equals(diagnostic.get("ready")) ? 200 : 503, diagnostic);
                }
                case "/metrics" -> {
                    method(exchange, "GET");
                    Map<String, Number> gauges = database.gauges();
                    gauges.put("http_workers", config.workers());
                    gauges.put("http_workers_active", workers.getActiveCount());
                    gauges.put("http_queue", workers.getQueue().size());
                    gauges.put("admin_workers", 16);
                    gauges.put("admin_workers_active", adminWorkers.getActiveCount());
                    gauges.put("admin_queue", adminWorkers.getQueue().size());
                    gauges.put("inflight", inflight.get());
                    gauges.put("hold_active", holdActive.get());
                    gauges.put("hold_peak", holdPeak.get());
                    text(exchange, 200, metrics.render(gauges), "text/plain; version=0.0.4; charset=utf-8");
                }
                case "/work" -> {
                    method(exchange, "GET");
                    String op = query.getOrDefault("op", "browse");
                    if (!Metrics.OPS.contains(op) || Set.of("hold", "rollback").contains(op))
                        throw new IllegalArgumentException("op must be browse|history|checkout|review|account|report");
                    Metrics.Context context = context(query);
                    long seed = Long.parseLong(query.getOrDefault("seed", "0"));
                    admitted(exchange, () -> database.work(op, seed, context, false));
                }
                case "/hold" -> {
                    method(exchange, "GET");
                    testEnabled();
                    int millis = Integer.parseInt(query.getOrDefault("millis", "1000"));
                    if (millis < 0 || millis > 30000) throw new IllegalArgumentException("millis outside 0..30000");
                    Metrics.Context context = context(query);
                    admitted(exchange, () -> {
                        Metrics.Stats stats = metrics.stats(database.mode.equals("ojp") ? "ojp" : "direct", "hold", context);
                        long start = System.nanoTime();
                        int active = holdActive.incrementAndGet(); holdPeak.accumulateAndGet(active, Math::max);
                        try {
                            Thread.sleep(millis); stats.completed.increment();
                            return Map.of("ok", true, "active", active, "peak", holdPeak.get(), "millis", millis);
                        } finally { holdActive.decrementAndGet(); stats.request.record(System.nanoTime() - start); }
                    });
                }
                case "/resources" -> {
                    method(exchange, "POST");
                    if (!authorized(exchange)) { send(exchange, 401, Map.of("error", "Admin authorization required")); return; }
                    metrics.resources(Double.parseDouble(query.getOrDefault("cpu_percent", "")),
                            Double.parseDouble(query.getOrDefault("memory_percent", "")));
                    send(exchange, 200, Map.of("ok", true));
                }
                case "/arrivals" -> {
                    method(exchange, "POST");
                    if (!authorized(exchange)) { send(exchange, 401, Map.of("error", "Admin authorization required")); return; }
                    Metrics.Context context = context(query);
                    Metrics.Arrivals update = new Metrics.Arrivals(
                            count(query, "scheduled"), count(query, "dispatched"), count(query, "dropped"),
                            count(query, "inflight"), count(query, "completed"), count(query, "errors"),
                            count(query, "accepted"), count(query, "sql_completed"), count(query, "timeouts"), latency(query));
                    for (String alias : List.of("successful_transactions", "failed_transactions", "http_completed")) {
                        if (query.containsKey(alias)) {
                            long expected = alias.equals("successful_transactions") ? update.completed()
                                    : alias.equals("failed_transactions") ? update.errors() : update.completed() + update.errors();
                            if (count(query, alias) != expected) throw new IllegalArgumentException("Inconsistent " + alias);
                        }
                    }
                    if (Boolean.parseBoolean(query.getOrDefault("final", "false"))
                            && (update.inflight() != 0 || update.scheduled() != update.dispatched() + update.dropped()
                            || update.dispatched() != update.completed() + update.errors()))
                        throw new IllegalArgumentException("Final arrivals must be fully drained and accounted for");
                    metrics.arrivals(query.getOrDefault("mode", database.mode), context, update,
                            Boolean.parseBoolean(query.getOrDefault("reset", "false")));
                    send(exchange, 200, Map.of("ok", true, "scheduled", update.scheduled(), "dispatched", update.dispatched(),
                            "dropped", update.dropped(), "inflight", update.inflight()));
                }
                case "/benchmark-result" -> {
                    method(exchange, "POST");
                    if (!authorized(exchange)) { send(exchange, 401, Map.of("error", "Admin authorization required")); return; }
                    Metrics.Context context = context(query);
                    metrics.benchmarkResult(query.getOrDefault("mode", ""),
                            context, new Metrics.BenchmarkResult(
                                    decimal(query, "successful_rps"), decimal(query, "oltp_p95_ms"),
                                    decimal(query, "olap_p95_ms"), decimal(query, "error_percent"),
                                    decimal(query, "dropped_requests"), decimal(query, "physical_connections"),
                                    decimal(query, "postgres_cpu_percent"), decimal(query, "slow_classifications")));
                    send(exchange, 200, Map.of("ok", true));
                }
                case "/mode", "/reset", "/context", "/rollback" -> {
                    method(exchange, "POST");
                    if (!authorized(exchange)) { send(exchange, 401, Map.of("error", "Admin authorization required")); return; }
                    if (!mutation.tryAcquire()) { send(exchange, 409, Map.of("error", "Admin mutation already in progress")); return; }
                    try {
                        switch (path) {
                            case "/mode" -> {
                                database.switchMode(query.getOrDefault("value", ""));
                                send(exchange, 200, database.diagnostics(true));
                            }
                            case "/reset" -> { database.reset(); send(exchange, 200, Map.of("ok", true)); }
                            case "/context" -> {
                                String nextRunId = query.getOrDefault("run_id", "");
                                if (nextRunId.length() > 64 || nextRunId.chars().anyMatch(c -> c < 32))
                                    throw new IllegalArgumentException("run_id must be at most64 printable characters");
                                metrics.updateContext(context(query), !Objects.equals(runId, nextRunId));
                                runId = nextRunId;
                                send(exchange, 200, Map.of("ok", true, "phase", metrics.context.phase(),
                                        "scenario", metrics.context.scenario(), "level", metrics.context.level(), "run_id", runId));
                            }
                            case "/rollback" -> {
                                testEnabled();
                                send(exchange, 200, database.work("rollback", Long.parseLong(query.getOrDefault("seed", "0")),
                                        context(query), true));
                            }
                            default -> throw new IllegalStateException();
                        }
                    } finally { mutation.release(); }
                }
                default -> send(exchange, 404, Map.of("error", "Not found"));
            }
        } catch (MethodException e) {
            send(exchange, 405, Map.of("error", "Method not allowed"));
        } catch (IllegalArgumentException e) {
            send(exchange, 400, Map.of("ok", false, "error", e.getMessage()));
        } catch (DisabledException e) {
            send(exchange, 404, Map.of("error", "Test endpoints disabled"));
        } catch (IllegalStateException e) {
            Map<String, Object> failure = new LinkedHashMap<>(
                    Map.of("ok", false, "error", e.getMessage()));
            if (path.equals("/mode")) {
                Map<String, Object> diagnostic = database.diagnostics(false);
                for (String key : List.of("state", "diagnostic", "direct_sessions",
                        "direct_application_name", "last_error"))
                    failure.put(key, diagnostic.get(key));
            }
            send(exchange, 503, failure);
        } catch (Exception e) {
            String code = errorCode(e);
            Map<String, Object> failure = new LinkedHashMap<>(Map.of(
                    "ok", false, "error", "Transaction/control failed",
                    "code", Objects.toString(code, "unknown")));
            if (path.equals("/mode")) {
                Map<String, Object> diagnostic = database.diagnostics(false);
                for (String key : List.of("state", "diagnostic", "direct_sessions",
                        "direct_application_name", "last_error"))
                    failure.put(key, diagnostic.get(key));
            }
            send(exchange, 503, failure);
        } finally { exchange.close(); }
    }

    static String errorCode(Exception error) {
        if (error instanceof SQLException sql) {
            String state = sql.getSQLState();
            // OJP admission timeouts retain their JDBC subtype but may omit SQLState.
            if ((state == null || state.isBlank()) && sql instanceof SQLTransientConnectionException)
                return "08001";
            return Objects.toString(state, "unknown");
        }
        return error.getClass().getSimpleName();
    }

    private void admitted(HttpExchange exchange, Work work) throws Exception {
        if (!admission.tryAcquire()) {
            metrics.rejected.increment();
            send(exchange, 503, Map.of("ok", false, "error", "Request admission full")); return;
        }
        inflight.incrementAndGet();
        try { send(exchange, 200, work.run()); }
        finally { inflight.decrementAndGet(); admission.release(); }
    }

    private Metrics.Context context(Map<String, String> query) {
        Metrics.Context c = metrics.context;
        return metrics.context(query.getOrDefault("phase", c.phase()), query.getOrDefault("scenario", c.scenario()),
                Double.parseDouble(query.getOrDefault("level", Double.toString(c.level()))));
    }
    private static long count(Map<String, String> query, String key) {
        return Long.parseLong(query.getOrDefault(key, "0"));
    }
    private static double decimal(Map<String, String> query, String key) {
        return Double.parseDouble(query.getOrDefault(key, ""));
    }
    private static Metrics.Latency latency(Map<String, String> query) {
        if (!query.containsKey("latency_bucket_counts") && !query.containsKey("latency_count")
                && !query.containsKey("latency_sum_seconds")) return null;
        List<Long> buckets = Arrays.stream(query.getOrDefault("latency_bucket_counts", "").split(",", -1))
                .map(Long::parseLong).toList();
        return new Metrics.Latency(count(query, "latency_count"),
                Double.parseDouble(query.getOrDefault("latency_sum_seconds", "0")), buckets);
    }

    static Map<String, String> query(String raw) {
        if (raw == null || raw.isEmpty()) return Map.of();
        if (raw.length() > 2048) throw new IllegalArgumentException("Query too long");
        Map<String, String> result = new HashMap<>();
        for (String pair : raw.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length == 2 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            if (result.put(key, value) != null) throw new IllegalArgumentException("Duplicate query parameter");
        }
        return result;
    }

    private boolean authorized(HttpExchange exchange) {
        String token = System.getenv().getOrDefault("ADMIN_TOKEN", "");
        return token.isBlank() || MessageDigest.isEqual(("Bearer " + token).getBytes(StandardCharsets.UTF_8),
                exchange.getRequestHeaders().getFirst("Authorization") == null ? new byte[0]
                        : exchange.getRequestHeaders().getFirst("Authorization").getBytes(StandardCharsets.UTF_8));
    }
    private void testEnabled() { if (!config.testEndpoints()) throw new DisabledException(); }
    private static void method(HttpExchange exchange, String expected) {
        if (!expected.equals(exchange.getRequestMethod())) {
            exchange.getResponseHeaders().set("Allow", expected); throw new MethodException();
        }
    }
    private static void send(HttpExchange exchange, int status, Map<String, ?> body) throws IOException {
        text(exchange, status, Json.encode(body), "application/json; charset=utf-8");
    }
    private static void text(HttpExchange exchange, int status, String body, String contentType) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }
    @FunctionalInterface private interface Work { Map<String, Object> run() throws Exception; }
    private static final class MethodException extends RuntimeException {}
    private static final class DisabledException extends RuntimeException {}

    @Override public void close() {
        workServer.stop(1); adminServer.stop(1);
        workers.shutdownNow(); adminWorkers.shutdownNow(); database.close();
    }
    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            switch (args[0]) {
                case "--help" -> System.out.println("""
                        OJP live demo (Java 21)
                        java -jar demo.jar [--help|--version|--validate-config]
                        Env: PG_URL PG_USER PG_PASSWORD PG_ADMIN_URL PG_ADMIN_USER PG_ADMIN_PASSWORD
                             OJP_URL MODE=direct|ojp DIRECT_POOL_SIZE=200 HTTP_WORKERS=512
                             OJP_POOL_SIZE=10 OJP_MIN_IDLE=10
                             PORT=8080 ADMIN_PORT=8081 HTTP_QUEUE_SIZE=1024 STARTUP_TIMEOUT_SECONDS=180
                             SQL_TIMEOUT_SECONDS=30 BIND_ADDRESS=0.0.0.0 TEST_ENDPOINTS=false ADMIN_TOKEN
                        Admin mutations are demo-only: protect both ports; set ADMIN_TOKEN outside local demos.
                        """);
                case "--version" -> System.out.println("ojp-live-demo 1.0.0; OJP JDBC 1.0.3");
                case "--validate-config" -> {
                    Config c = Config.load(System.getenv());
                    System.out.println(Json.encode(Map.of("valid", true, "java", Runtime.version().feature(),
                            "workers", c.workers(), "direct_pool_size", c.poolSize(),
                            "ojp_pool_max", c.ojpPoolSize(), "ojp_pool_min", c.ojpMinIdle())));
                }
                case "--healthcheck" -> {
                    Config c = Config.load(System.getenv());
                    try (HttpClient client = HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(2)).build()) {
                        int status = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + c.adminPort() + "/health"))
                                .timeout(java.time.Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.discarding()).statusCode();
                        if (status != 200) throw new IllegalStateException("Healthcheck HTTP " + status);
                    }
                }
                default -> throw new IllegalArgumentException("Unknown option; use --help");
            }
            return;
        }
        Main app = new Main(Config.load(System.getenv()));
        Runtime.getRuntime().addShutdownHook(new Thread(app::close, "shutdown"));
        app.start(true);
    }
}
