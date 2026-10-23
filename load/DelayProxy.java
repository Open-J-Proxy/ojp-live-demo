import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Executors;

final HttpClient client = HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.NORMAL).connectTimeout(Duration.ofSeconds(60)).build();
final String target = System.getenv("TARGET_APP_URL");

void main() throws Exception {
    if (target == null || target.isBlank()) {
        throw new IllegalArgumentException("TARGET_APP_URL is required");
    }
    int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
    var server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    server.createContext("/", this::forward);
    server.start();
}

void forward(HttpExchange exchange) {
    try (exchange) {
        try {
            String path = exchange.getRequestURI().toString();
            if (path.startsWith("/work?")) Thread.sleep(250);
            byte[] body = exchange.getRequestBody().readAllBytes();
            var request = HttpRequest.newBuilder(URI.create(target.replaceFirst("/+$", "") + path))
                    .timeout(Duration.ofSeconds(60))
                    .method(exchange.getRequestMethod(), body.length == 0
                            ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body));
            for (String header : new String[] {"Authorization", "Content-Type"}) {
                String value = exchange.getRequestHeaders().getFirst(header);
                if (value != null) request.header(header, value);
            }
            var response = client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
            response.headers().firstValue("Content-Type")
                    .ifPresent(value -> exchange.getResponseHeaders().set("Content-Type", value));
            boolean noBody = exchange.getRequestMethod().equals("HEAD")
                    || response.statusCode() == 204 || response.statusCode() == 304;
            exchange.sendResponseHeaders(response.statusCode(), noBody ? -1 : response.body().length);
            if (!noBody) exchange.getResponseBody().write(response.body());
        } catch (Exception error) {
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            exchange.sendResponseHeaders(502, -1);
        }
    } catch (Exception ignored) {
        // A disconnected test client must not stop the proxy.
    }
}
