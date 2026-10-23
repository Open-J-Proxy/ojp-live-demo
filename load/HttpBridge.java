import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
final Object outputLock = new Object();

void main() throws Exception {
    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
         var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
        String line;
        while ((line = input.readLine()) != null) {
            String message = line;
            executor.submit(() -> request(message));
        }
    }
}

void request(String line) {
    var result = new LinkedHashMap<String, Object>();
    try {
        var message = (Map<?, ?>) LoadJson.parse(line);
        result.put("id", message.get("id"));
        Object payload = message.get("data");
        Object method = message.get("method");
        var timeout = Duration.ofNanos((long) (((Number) message.get("timeout")).doubleValue() * 1e9));
        var request = HttpRequest.newBuilder(URI.create((String) message.get("url")))
                .timeout(timeout)
                .method(method == null ? (payload == null ? "GET" : "POST") : (String) method,
                        payload == null ? HttpRequest.BodyPublishers.noBody()
                                : HttpRequest.BodyPublishers.ofString(LoadJson.stringify(payload), StandardCharsets.UTF_8));
        if (message.get("headers") instanceof Map<?, ?> headers) {
            for (var header : headers.entrySet()) {
                request.header((String) header.getKey(), (String) header.getValue());
            }
        }
        // HttpRequest.timeout alone does not bound reading a response body.
        var future = client.sendAsync(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        HttpResponse<String> response;
        try {
            response = future.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException | InterruptedException error) {
            future.cancel(true);
            throw error;
        }
        result.put("status", response.statusCode());
        result.put("body", response.body());
    } catch (Exception error) {
        if (error instanceof InterruptedException) Thread.currentThread().interrupt();
        // Exception messages may contain URLs or authorization values.
        result.put("error", "HTTP request failed (" + error.getClass().getSimpleName() + ")");
    }
    synchronized (outputLock) {
        System.out.println(LoadJson.stringify(result));
        System.out.flush();
    }
}
