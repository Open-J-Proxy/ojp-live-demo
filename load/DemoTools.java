import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Shared host-side utilities for the dependency-free source launchers. */
public final class DemoTools {
    private DemoTools() {}

    public static Path root() {
        String configured = System.getenv("OJP_DEMO_ROOT");
        if (configured != null && !configured.isBlank()) return Path.of(configured).toAbsolutePath().normalize();
        String source = System.getProperty("jdk.launcher.sourcefile");
        if (source != null) {
            Path parent = Path.of(source).toAbsolutePath().normalize().getParent();
            if (parent != null && parent.getFileName().toString().equals("load")) return parent.getParent();
        }
        for (Path path = Path.of("").toAbsolutePath(); path != null; path = path.getParent()) {
            if (Files.isRegularFile(path.resolve("compose.yaml")) && Files.isDirectory(path.resolve("load"))) return path;
        }
        throw new IllegalStateException("Cannot locate repository; set OJP_DEMO_ROOT");
    }

    public static List<String> compose(Map<String, String> env) {
        var result = new ArrayList<>(List.of("docker", "compose", "--project-directory", root().toString(),
                "-f", root().resolve("compose.yaml").toString()));
        String override = env.get("COMPOSE_OVERRIDE_FILE");
        if (override != null && !override.isBlank()) result.addAll(List.of("-f", override));
        return result;
    }

    public static List<String> command(List<String> prefix, String... suffix) {
        var command = new ArrayList<>(prefix);
        command.addAll(Arrays.asList(suffix));
        return command;
    }

    private static ProcessBuilder builder(List<String> command, Map<String, String> env) {
        var builder = new ProcessBuilder(command).directory(root().toFile()).redirectErrorStream(true);
        if (env != null) {
            builder.environment().clear();
            builder.environment().putAll(env);
        }
        return builder;
    }

    public record Result(int exitCode, String output) {
        public Result check() throws IOException {
            if (exitCode != 0) throw new IOException("Command exited " + exitCode + ": " + output);
            return this;
        }
    }

    public static Result run(List<String> command, Map<String, String> env, double timeout) throws Exception {
        Process process = builder(command, env).start();
        var output = new ByteArrayOutputStream();
        Thread reader = Thread.ofVirtual().start(() -> {
            try (var input = process.getInputStream()) { input.transferTo(output); }
            catch (IOException ignored) {}
        });
        try {
            if (!process.waitFor(nanos(timeout), TimeUnit.NANOSECONDS)) {
                terminate(process, 2);
                throw new IOException("Command timed out after " + timeout + " seconds: " + String.join(" ", command));
            }
            reader.join(5000);
            if (reader.isAlive()) throw new IOException("Command output did not close");
            return new Result(process.exitValue(), output.toString(StandardCharsets.UTF_8));
        } finally {
            if (process.isAlive()) terminate(process, 2);
            if (reader.isAlive()) process.getInputStream().close();
        }
    }

    public static Process start(List<String> command, Map<String, String> env, Path log) throws IOException {
        return builder(command, env).redirectOutput(log.toFile()).start();
    }

    public static void terminate(Process process, double grace) throws InterruptedException {
        if (process == null) return;
        var descendants = process.descendants().toList();
        descendants.forEach(ProcessHandle::destroy);
        process.destroy();
        if (!process.waitFor(nanos(grace), TimeUnit.NANOSECONDS)) {
            descendants.forEach(ProcessHandle::destroyForcibly);
            process.destroyForcibly();
            process.waitFor(5, TimeUnit.SECONDS);
        }
        descendants.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
    }

    private static long nanos(double seconds) {
        if (!Double.isFinite(seconds) || seconds <= 0) throw new IllegalArgumentException("Timeout must be finite and positive");
        return Math.max(1, (long) (seconds * 1e9));
    }

    public static String safeId(String id) {
        String safe = id.replaceAll("[^a-zA-Z0-9_.-]", "_");
        return safe.isEmpty() || safe.equals(".") || safe.equals("..") ? "run_" + safe.replace('.', '_') : safe;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> map(Object value) {
        return value instanceof Map<?, ?> ? (Map<String, Object>) value : Map.of();
    }

    public static String localUser(Map<String, String> env) throws Exception {
        String uid = env.get("LOCAL_UID"), gid = env.get("LOCAL_GID");
        if (uid == null) uid = run(List.of("id", "-u"), env, 5).check().output().strip();
        if (gid == null) gid = run(List.of("id", "-g"), env, 5).check().output().strip();
        return uid + ":" + gid;
    }

    public static String applicationUrl(Map<String, String> env) {
        return env.getOrDefault("APP_URL", env.getOrDefault("APP_PORT", "8080").equals("0")
                ? env.getOrDefault("APP_INTERNAL_URL", "http://app:8080")
                : "http://127.0.0.1:" + env.getOrDefault("APP_PORT", "8080"));
    }

    public static Map<String, String> postHeaders(Map<String, String> env) {
        String token = env.get("ADMIN_TOKEN");
        return token == null || token.isEmpty() ? Map.of() : Map.of("Authorization", "Bearer " + token);
    }

    public record Response(int status, Object body) {}
    public interface Transport extends AutoCloseable {
        Response request(String url, Object data, String method, double timeout, Map<String, String> headers) throws Exception;
        @Override default void close() throws Exception {}
    }

    public static Transport transport(Map<String, String> env) throws Exception {
        return "0".equals(env.get("APP_PORT")) && !env.containsKey("APP_URL") ? new Bridge(env) : DemoTools::http;
    }

    private static final HttpClient CLIENT = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();

    public static Response http(String url, Object data, String method, double timeout,
                                Map<String, String> headers) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofNanos(nanos(timeout)))
                .method(method == null ? (data == null ? "GET" : "POST") : method,
                        data == null ? HttpRequest.BodyPublishers.noBody()
                                : HttpRequest.BodyPublishers.ofString(LoadJson.stringify(data), StandardCharsets.UTF_8));
        headers.forEach(request::header);
        var future = CLIENT.sendAsync(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        try {
            var response = future.get(nanos(timeout), TimeUnit.NANOSECONDS);
            return new Response(response.statusCode(), parsed(response.body()));
        } catch (TimeoutException | InterruptedException error) {
            future.cancel(true);
            throw error;
        }
    }

    private static Object parsed(String body) {
        try { return LoadJson.parse(body); }
        catch (IllegalArgumentException ignored) { return body; }
    }

    public static final class Bridge implements Transport {
        private final Map<String, String> env;
        private final String name;
        private final Process process;
        private final BufferedWriter input;
        private final Thread reader;
        private final Object lock = new Object();
        private final Map<Long, CompletableFuture<Map<String, Object>>> pending = new HashMap<>();
        private final AtomicLong ids = new AtomicLong();
        private boolean closed;

        public Bridge(Map<String, String> env) throws IOException {
            this.env = Map.copyOf(env);
            name = env.getOrDefault("COMPOSE_PROJECT_NAME", "ojp-demo") + "-http-"
                    + ProcessHandle.current().pid() + "-" + UUID.randomUUID().toString().substring(0, 8);
            Path output = Path.of(env.getOrDefault("RESULTS_DIR", root().resolve("results").toString()));
            Files.createDirectories(output);
            var command = command(compose(env), "--profile", "tools", "run", "--rm", "--no-deps", "-T",
                    "--name", name, "--entrypoint", "java", "load", "/app/HttpBridge.java");
            var builder = builder(command, env).redirectErrorStream(false)
                    .redirectError(output.resolve("http-client-" + name + ".log").toFile());
            process = builder.start();
            input = process.outputWriter(StandardCharsets.UTF_8);
            reader = Thread.ofVirtual().start(this::read);
        }

        private void read() {
            try (var output = process.inputReader(StandardCharsets.UTF_8)) {
                String line;
                while ((line = output.readLine()) != null) {
                    Map<String, Object> message;
                    try { message = map(LoadJson.parse(line)); }
                    catch (IllegalArgumentException ignored) { continue; }
                    if (!(message.get("id") instanceof Number id)) continue;
                    CompletableFuture<Map<String, Object>> future;
                    synchronized (lock) { future = pending.remove(id.longValue()); }
                    if (future != null) future.complete(message);
                }
            } catch (IOException ignored) {
            } finally {
                synchronized (lock) {
                    closed = true;
                    pending.values().forEach(future -> future.completeExceptionally(
                            new IOException("Internal HTTP client exited; inspect http-client log")));
                    pending.clear();
                }
            }
        }

        @Override public Response request(String url, Object data, String method, double timeout,
                                          Map<String, String> headers) throws Exception {
            long id = ids.getAndIncrement();
            var future = new CompletableFuture<Map<String, Object>>();
            var message = new LinkedHashMap<String, Object>();
            message.put("id", id); message.put("url", url); message.put("data", data);
            message.put("method", method); message.put("timeout", timeout); message.put("headers", headers);
            synchronized (lock) {
                if (closed || !process.isAlive()) throw new IOException("Internal HTTP client is not running");
                pending.put(id, future);
                try {
                    input.write(LoadJson.stringify(message)); input.newLine(); input.flush();
                } catch (IOException error) { pending.remove(id); throw error; }
            }
            try {
                var response = future.get(nanos(timeout), TimeUnit.NANOSECONDS);
                if (response.containsKey("error")) throw new IOException(response.get("error").toString());
                return new Response(((Number) response.get("status")).intValue(), parsed((String) response.get("body")));
            } catch (TimeoutException | InterruptedException error) {
                future.cancel(true);
                throw error;
            } finally { synchronized (lock) { pending.remove(id); } }
        }

        @Override public void close() throws Exception {
            IOException inputFailure = null;
            synchronized (lock) {
                closed = true;
                pending.values().forEach(future -> future.completeExceptionally(new IOException("HTTP bridge closed")));
                pending.clear();
                try { input.close(); }
                catch (IOException error) { inputFailure = error; }
            }
            try {
                if (!process.waitFor(10, TimeUnit.SECONDS)) terminate(process, 5);
            } finally {
                try { run(List.of("docker", "rm", "-f", name), env, 10); }
                finally { reader.join(5000); }
            }
            if (inputFailure != null) throw inputFailure;
        }
    }

    public static void main(String[] args) {
        System.out.println("Shared demo host utilities; repository: " + root());
    }
}
