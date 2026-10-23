package demo;

import org.junit.jupiter.api.Test;
import java.net.URI;
import java.net.ServerSocket;
import java.net.http.*;
import java.time.Duration;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class AppTest {
    @Test void defaultsAndDriverProperties() throws Exception {
        Config c = Config.load(Map.of());
        assertEquals(200, c.poolSize()); assertEquals(512, c.workers());
        assertEquals("10", c.ojpProperties().getProperty("ojp.connection.pool.maximumPoolSize"));
        assertEquals("10", c.ojpProperties().getProperty("ojp.connection.pool.minimumIdle"));
        assertEquals("true", c.ojpProperties().getProperty("ojp.jdbc.connection.close.synchronous"));
        assertTrue(c.ojpUrl().endsWith("ApplicationName=demo-ojp"));
        assertEquals("jdbc:postgresql://localhost/shop?sslmode=disable&ApplicationName=demo-admin",
                Config.applicationName("jdbc:postgresql://localhost/shop?ApplicationName=old&sslmode=disable&applicationname=other", "demo-admin"));
        assertNotNull(Class.forName("org.openjproxy.jdbc.Driver"));
        assertThrows(IllegalArgumentException.class, () -> Config.load(Map.of("HTTP_WORKERS", "199")));
        assertThrows(IllegalArgumentException.class, () -> Config.load(Map.of("MODE", "bad")));
        Config aliases = Config.load(Map.of("DB_URL", "jdbc:postgresql://localhost:5432/test", "DB_USER", "test",
                "DB_PASSWORD", "local", "HTTP_QUEUE_CAPACITY", "999", "INIT_TIMEOUT_SECONDS", "77"));
        assertEquals("test", aliases.pgUser()); assertEquals(999, aliases.queueSize()); assertEquals(77, aliases.startupSeconds());
        Config smallerOjp = Config.load(Map.of("OJP_POOL_SIZE", "16", "OJP_MIN_IDLE", "4"));
        assertEquals("16", smallerOjp.ojpProperties().getProperty("ojp.connection.pool.maximumPoolSize"));
        assertEquals("4", smallerOjp.ojpProperties().getProperty("ojp.connection.pool.minimumIdle"));
        assertThrows(IllegalArgumentException.class, () -> Config.load(Map.of("OJP_POOL_SIZE", "4", "OJP_MIN_IDLE", "10")));
    }

    @Test void boundedLabelsAndHistogram() {
        Metrics m = new Metrics();
        assertThrows(IllegalArgumentException.class, () -> m.context("measure", "arbitrary", 0));
        assertThrows(IllegalArgumentException.class, () -> m.context("measure", "integration", 10001));
        assertThrows(IllegalArgumentException.class, () -> m.context("measure", "integration", Double.NaN));
        for (int i = 0; i < 64; i++) m.context("warmup", "integration", i);
        assertThrows(IllegalArgumentException.class, () -> m.context("measure", "integration", 65));
        var stats = m.stats("direct", "browse", m.context("measure", "integration", 1));
        stats.completed.increment(); stats.request.record(10_000_000);
        String text = m.render(Map.of());
        assertTrue(text.contains("demo_completions_total{mode=\"direct\",op=\"browse\",phase=\"measure\",scenario=\"integration\",level=\"1\"} 1"));
        m.connectionOpened("direct");
        m.connectionClosed("direct");
        m.connectionOpened("ojp");
        m.physicalConnections(200, 40);
        text = m.render(Map.of());
        assertTrue(text.contains("demo_logical_connections_opened_total{mode=\"direct\"} 1"));
        assertTrue(text.contains("demo_logical_connections_closed_total{mode=\"direct\"} 1"));
        assertTrue(text.contains("demo_logical_connections_opened_total{mode=\"ojp\"} 1"));
        assertTrue(text.contains("demo_postgres_physical_connections{mode=\"direct\"} 200"));
        assertTrue(text.contains("demo_postgres_physical_connections{mode=\"ojp\"} 40"));
        assertFalse(text.contains("demo_logical_connections_opened_total 2"));
        assertTrue(text.contains("le=\"+Inf\"} 1"));
        assertTrue(text.contains("le=\"0.001\"} 0"));
        m.updateContext(m.context("warmup", "integration", 1), true);
        assertTrue(m.render(Map.of()).contains("demo_measurement_start_timestamp_seconds 0.0\n"));
        m.mode = "ojp";
        m.updateContext(m.context("measure", "integration", 1), false);
        assertTrue(m.render(Map.of()).contains("demo_context{mode=\"ojp\",phase=\"measure\",scenario=\"integration\",level=\"1\"} 1"));
        assertFalse(m.render(Map.of()).contains("demo_measurement_start_timestamp_seconds 0.0\n"));
    }

    @Test void parserAndJson() {
        assertEquals("a b", Main.query("op=a+b").get("op"));
        assertThrows(IllegalArgumentException.class, () -> Main.query("op=a&op=b"));
        assertEquals("\"\\\"\\n\\u0001\"", Json.encode("\"\n\u0001"));
    }

    @Test void admissionTimeoutWithoutSqlStateRetainsConnectionFailureCode() {
        assertEquals("08001", Main.errorCode(new SQLTransientConnectionException("admission timeout")));
        assertEquals("08001", Main.errorCode(new SQLTransientConnectionException("admission timeout", "")));
        assertEquals("08001", Main.errorCode(new SQLTransientConnectionException("admission timeout", " ")));
        assertEquals("08003", Main.errorCode(new SQLTransientConnectionException("closed connection", "08003")));
        assertEquals("42601", Main.errorCode(new SQLException("syntax error", "42601")));
        assertEquals("", Main.errorCode(new SQLException("unspecified SQL failure", "")));
        assertEquals("unknown", Main.errorCode(new SQLException("unspecified SQL failure")));
        assertEquals("IOException", Main.errorCode(new java.io.IOException("transport failure")));
    }

    @Test void startupDiagnosticsPreservePostgresFailureCause() {
        Config config = Config.load(Map.of("PG_URL", "jdbc:postgresql://127.0.0.1:1/shop",
                "STARTUP_TIMEOUT_SECONDS", "1"));
        try (Database database = new Database(config, new Metrics())) {
            database.initialize();
            Map<String, Object> diagnostic = database.diagnostics(false);
            assertEquals(false, diagnostic.get("ready"));
            assertEquals("failed", diagnostic.get("state"));
            assertTrue(diagnostic.get("last_error").toString().contains("PSQLException"));
            assertTrue(diagnostic.get("last_error").toString().contains("SQLState=08001"));
        }
    }

    @Test void offeredLoadTelemetryIsBoundedAndMonotonic() {
        Metrics m = new Metrics();
        Metrics.Context context = m.context("measure", "integration", 20);
        m.arrivals("direct", context, new Metrics.Arrivals(10, 8, 1, 3, 4, 1), false);
        String metrics = m.render(Map.of());
        assertTrue(metrics.contains("benchmark_load_scheduled_total{mode=\"direct\",phase=\"measure\",scenario=\"integration\",level=\"20\"} 10"));
        assertTrue(metrics.contains("demo_load_dispatched_total{mode=\"direct\",phase=\"measure\",scenario=\"integration\",level=\"20\"} 8"));
        assertThrows(IllegalArgumentException.class,
                () -> m.arrivals("direct", context, new Metrics.Arrivals(9, 8, 1, 2, 5, 1), false));
        assertThrows(IllegalArgumentException.class, () -> new Metrics.Arrivals(2, 2, 1, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new Metrics.Arrivals(2, 2, 0, 3, 0, 0));
        m.arrivals("direct", context, new Metrics.Arrivals(0, 0, 0, 0, 0, 0), true);
        assertTrue(m.render(Map.of()).contains("level=\"20\"} 0"));
        m.arrivals("ojp", m.context("measure", "integration", 12.5), new Metrics.Arrivals(1, 1, 0, 0, 1, 0), false);
        assertTrue(m.render(Map.of()).contains("level=\"12.5\"} 1"));
        List<Long> buckets = new ArrayList<>(Collections.nCopies(14, 0L));
        buckets.set(13, 1L);
        Metrics.Latency latency = new Metrics.Latency(1, 80, buckets);
        m.arrivals("ojp", m.context("measure", "integration", 12.5),
                new Metrics.Arrivals(1, 1, 0, 0, 1, 0, 1, 1, 0, latency), false);
        String histogram = m.render(Map.of());
        assertTrue(histogram.contains("benchmark_load_latency_seconds_bucket{mode=\"ojp\",phase=\"measure\",scenario=\"integration\",level=\"12.5\",le=\"+Inf\"} 1"));
        assertTrue(histogram.contains("benchmark_load_latency_seconds_sum{mode=\"ojp\",phase=\"measure\",scenario=\"integration\",level=\"12.5\"} 80.0"));
        assertThrows(IllegalArgumentException.class, () -> new Metrics.Latency(2, 80, buckets));
        assertThrows(IllegalArgumentException.class, () -> new Metrics.Latency(1, Double.NaN, buckets));
        buckets.set(3, 2L);
        assertThrows(IllegalArgumentException.class, () -> new Metrics.Latency(1, 80, buckets));
    }

    @Test void measuredBenchmarkResultsAreModeAndLevelScoped() {
        Metrics metrics = new Metrics();
        Metrics.Context context = metrics.context("measure", "htap", 16);
        metrics.benchmarkResult("direct", context, new Metrics.BenchmarkResult(
                15, 20, 200, 1, 2, 200, 65, 0));
        metrics.benchmarkResult("ojp", context, new Metrics.BenchmarkResult(
                16, 15, 180, 0, 0, 40, 55, 3));
        String text = metrics.render(Map.of());
        assertTrue(text.contains("demo_benchmark_result_successful_rps{mode=\"direct\",phase=\"measure\",scenario=\"htap\",level=\"16\"} 15.0"));
        assertTrue(text.contains("demo_benchmark_result_physical_connections{mode=\"ojp\",phase=\"measure\",scenario=\"htap\",level=\"16\"} 40.0"));
        assertThrows(IllegalArgumentException.class, () -> metrics.benchmarkResult("direct",
                metrics.context("warmup", "htap", 16), new Metrics.BenchmarkResult(1, 1, 1, 0, 0, 1, 1, 0)));
    }

    @Test void twoHundredRequestsExecuteConcurrentlyWithoutDatabase() throws Exception {
        int port = freePort(), adminPort = freePort();
        while (port == adminPort) adminPort = freePort();
        Config c = Config.load(Map.of("PORT", Integer.toString(port), "ADMIN_PORT", Integer.toString(adminPort),
                "BIND_ADDRESS", "127.0.0.1", "TEST_ENDPOINTS", "true"));
        try (Main app = new Main(c);
             ExecutorService clientThreads = Executors.newVirtualThreadPerTaskExecutor()) {
            app.start(false);
            HttpClient client = HttpClient.newBuilder().executor(clientThreads).connectTimeout(Duration.ofSeconds(10)).build();
            HttpRequest.Builder telemetry = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + adminPort
                    + "/arrivals?mode=direct&phase=measure&scenario=integration&level=10&scheduled=10&dispatched=8&dropped=2&inflight=3&completed=4&errors=1"
                    + "&latency_count=5&latency_sum_seconds=0.01&latency_bucket_counts=5,5,5,5,5,5,5,5,5,5,5,5,5,5"));
            String token = System.getenv().getOrDefault("ADMIN_TOKEN", "");
            if (!token.isBlank()) telemetry.header("Authorization", "Bearer " + token);
            assertEquals(200, client.send(telemetry.POST(HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode());
            telemetry.uri(URI.create("http://127.0.0.1:" + adminPort + "/resources?cpu_percent=12.5&memory_percent=8.75"));
            assertEquals(200, client.send(telemetry.POST(HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode());
            telemetry.uri(URI.create("http://127.0.0.1:" + adminPort + "/resources?cpu_percent=NaN&memory_percent=8.75"));
            assertEquals(400, client.send(telemetry.POST(HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode());
            telemetry.uri(URI.create("http://127.0.0.1:" + adminPort
                    + "/context?phase=measure&scenario=integration&level=12.5&run_id=unit-run"));
            assertEquals(200, client.send(telemetry.POST(HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode());
            List<CompletableFuture<HttpResponse<String>>> holds = new ArrayList<>();
            for (int i = 0; i < 200; i++) holds.add(client.sendAsync(HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + port + "/hold?millis=4000")).GET().build(), HttpResponse.BodyHandlers.ofString()));
            String metrics = "";
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline) {
                metrics = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + adminPort + "/metrics"))
                        .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofString()).body();
                if (metrics.contains("demo_hold_peak 200\n")) break;
                Thread.sleep(50);
            }
            for (var hold : holds) assertEquals(200, hold.get(20, TimeUnit.SECONDS).statusCode());
            assertTrue(metrics.contains("demo_hold_peak 200\n"), metrics);
            assertTrue(metrics.contains("demo_logical_connections_opened_total{mode=\"direct\"} 0\n"));
            assertTrue(metrics.contains("demo_logical_connections_opened_total{mode=\"ojp\"} 0\n"));
            assertTrue(metrics.contains("demo_postgres_cpu_percent 12.5\n"));
            assertTrue(metrics.contains("demo_postgres_memory_percent 8.75\n"));
            assertTrue(metrics.contains("benchmark_load_scheduled_total{mode=\"direct\",phase=\"measure\",scenario=\"integration\",level=\"10\"} 10"));
            assertTrue(metrics.contains("benchmark_load_latency_seconds_count{mode=\"direct\",phase=\"measure\",scenario=\"integration\",level=\"10\"} 5"));
            HttpResponse<String> health = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + adminPort + "/health"))
                    .GET().build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, health.statusCode());
            assertTrue(health.body().contains("\"phase\":\"measure\""));
            assertTrue(health.body().contains("\"mode\":\"starting\""));
            assertTrue(health.body().contains("\"run_id\":\"unit-run\""));
            assertTrue(health.body().contains("\"level\":12.5"));
        }
    }
    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
    }
}
