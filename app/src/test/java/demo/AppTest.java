package demo;

import org.junit.jupiter.api.Test;
import java.net.URI;
import java.net.ServerSocket;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class AppTest {
    @Test void defaultsAndDriverProperties() throws Exception {
        Config c = Config.load(Map.of());
        assertEquals(200, c.poolSize()); assertEquals(512, c.workers());
        assertEquals("40", c.ojpProperties().getProperty("ojp.connection.pool.maximumPoolSize"));
        assertEquals("10", c.ojpProperties().getProperty("ojp.connection.pool.minimumIdle"));
        assertEquals("true", c.ojpProperties().getProperty("ojp.jdbc.connection.close.synchronous"));
        assertNotNull(Class.forName("org.openjproxy.jdbc.Driver"));
        assertThrows(IllegalArgumentException.class, () -> Config.load(Map.of("HTTP_WORKERS", "199")));
        assertThrows(IllegalArgumentException.class, () -> Config.load(Map.of("MODE", "bad")));
        Config aliases = Config.load(Map.of("DB_URL", "jdbc:postgresql://localhost:5432/test", "DB_USER", "test",
                "DB_PASSWORD", "local", "HTTP_QUEUE_CAPACITY", "999", "INIT_TIMEOUT_SECONDS", "77"));
        assertEquals("test", aliases.pgUser()); assertEquals(999, aliases.queueSize()); assertEquals(77, aliases.startupSeconds());
    }

    @Test void boundedLabelsAndHistogram() {
        Metrics m = new Metrics();
        assertThrows(IllegalArgumentException.class, () -> m.context("measure", "arbitrary", 0));
        assertThrows(IllegalArgumentException.class, () -> m.context("measure", "integration", 10001));
        for (int i = 0; i < 64; i++) m.context("warmup", "integration", i);
        assertThrows(IllegalArgumentException.class, () -> m.context("measure", "integration", 65));
        var stats = m.stats("direct", "browse", m.context("measure", "integration", 1));
        stats.completed.increment(); stats.request.record(10_000_000);
        String text = m.render(Map.of());
        assertTrue(text.contains("demo_completions_total{mode=\"direct\",op=\"browse\",phase=\"measure\",scenario=\"integration\",level=\"1\"} 1"));
        assertTrue(text.contains("le=\"+Inf\"} 1"));
        assertTrue(text.contains("le=\"0.001\"} 0"));
    }

    @Test void parserAndJson() {
        assertEquals("a b", Main.query("op=a+b").get("op"));
        assertThrows(IllegalArgumentException.class, () -> Main.query("op=a&op=b"));
        assertEquals("\"\\\"\\n\\u0001\"", Json.encode("\"\n\u0001"));
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
            assertTrue(metrics.contains("demo_logical_connections_opened_total 0\n"));
            assertEquals(200, client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + adminPort + "/health"))
                    .GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode());
        }
    }
    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
    }
}
