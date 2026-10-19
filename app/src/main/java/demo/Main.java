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
        try {
            Map<String, String> query = query(exchange.getRequestURI().getRawQuery());
            String path = exchange.getRequestURI().getPath();
            switch (path) {
                case "/health" -> {
                    method(exchange, "GET");
                    send(exchange, 200, Map.of("status", "up"));
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
                                metrics.context = context(query);
                                send(exchange, 200, Map.of("ok", true, "phase", metrics.context.phase(),
                                        "scenario", metrics.context.scenario(), "level", metrics.context.level()));
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
            send(exchange, 503, Map.of("ok", false, "error", e.getMessage()));
        } catch (Exception e) {
            String code = e instanceof SQLException sql ? sql.getSQLState() : e.getClass().getSimpleName();
            send(exchange, 503, Map.of("ok", false, "error", "Transaction/control failed", "code", Objects.toString(code, "unknown")));
        } finally { exchange.close(); }
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
                Integer.parseInt(query.getOrDefault("level", Integer.toString(c.level()))));
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
                             PORT=8080 ADMIN_PORT=8081 HTTP_QUEUE_SIZE=1024 STARTUP_TIMEOUT_SECONDS=180
                             SQL_TIMEOUT_SECONDS=30 BIND_ADDRESS=0.0.0.0 TEST_ENDPOINTS=false ADMIN_TOKEN
                        Admin mutations are demo-only: protect both ports; set ADMIN_TOKEN outside local demos.
                        """);
                case "--version" -> System.out.println("ojp-live-demo 1.0.0; OJP JDBC 1.0.3");
                case "--validate-config" -> {
                    Config c = Config.load(System.getenv());
                    System.out.println(Json.encode(Map.of("valid", true, "java", Runtime.version().feature(),
                            "workers", c.workers(), "direct_pool_size", c.poolSize(), "ojp_pool_max", 40, "ojp_pool_min", 10)));
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
