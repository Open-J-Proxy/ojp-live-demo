import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public static final class ResourceSampler implements AutoCloseable {
    public final Path directory;
    public final Path path;
    private final String runId;
    private final Map<String, String> env;
    private final CountDownLatch stopping = new CountDownLatch(1);
    private final Thread thread;
    private volatile boolean started;
    private DemoTools.Transport transport;

    public ResourceSampler(Path output, String runId, Map<String, String> env) throws IOException {
        this.runId = runId;
        this.env = Map.copyOf(env == null ? System.getenv() : env);
        directory = output.resolve(DemoTools.safeId(runId));
        Files.createDirectories(directory);
        path = directory.resolve("resources.jsonl");
        try { Files.createFile(path); }
        catch (FileAlreadyExistsException error) {
            throw new FileAlreadyExistsException("Resource evidence already exists; choose a new --run-id: " + runId);
        }
        thread = Thread.ofVirtual().unstarted(this::collect);
    }

    public synchronized void start() { thread.start(); started = true; }

    public void stop() throws Exception {
        stopping.countDown();
        if (started) {
            thread.join(25000);
            if (thread.isAlive()) { thread.interrupt(); thread.join(10000); }
            if (thread.isAlive()) throw new IOException("Resource sampler did not stop");
        }
        if (transport != null) { transport.close(); transport = null; }
        annotateIntervals();
    }

    @Override public void close() throws Exception { stop(); }

    private void collect() {
        try (var output = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            while (stopping.getCount() != 0) {
                String timestamp = Instant.now().toString();
                try {
                    String ids = DemoTools.run(DemoTools.command(DemoTools.compose(env),
                            "ps", "-q", "postgres", "app", "ojp"), env, 10).check().output().strip();
                    if (ids.isEmpty()) throw new IOException("No running postgres/app/ojp containers");
                    var command = new ArrayList<>(List.of("docker", "stats", "--no-stream", "--format", "{{json .}}"));
                    command.addAll(Arrays.asList(ids.split("\\s+")));
                    String stats = DemoTools.run(command, env, 10).check().output();
                    for (String line : stats.lines().filter(value -> !value.isBlank()).toList()) {
                        var record = new LinkedHashMap<String, Object>();
                        var values = DemoTools.map(LoadJson.parse(line));
                        record.put("timestamp", timestamp); record.put("run_id", runId);
                        record.put("source", "docker_stats"); record.put("sampling_scope", "whole_run");
                        record.put("collected_at", Instant.now().toString()); record.put("stats", values);
                        write(output, record);
                        if (values.getOrDefault("Name", "").toString().endsWith("-postgres-1")) {
                            try { publish(values); }
                            catch (Exception error) {
                                write(output, Map.of("timestamp", timestamp, "source", "docker_stats",
                                        "telemetry_error", error.toString()));
                            }
                        }
                    }
                } catch (Exception error) {
                    write(output, Map.of("timestamp", timestamp, "run_id", runId,
                            "source", "docker_stats", "error", error.toString()));
                    if (error instanceof InterruptedException) { Thread.currentThread().interrupt(); break; }
                }
                if (stopping.await(1, TimeUnit.SECONDS)) break;
            }
        } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
        catch (IOException error) { System.err.println("Resource evidence: " + error); }
    }

    private static void write(BufferedWriter output, Object value) throws IOException {
        output.write(LoadJson.stringify(value)); output.newLine(); output.flush();
    }

    private void publish(Map<String, Object> stats) throws Exception {
        String base;
        if ("0".equals(env.get("APP_PORT")) && !env.containsKey("APP_URL")) {
            base = DemoTools.applicationUrl(env);
        } else if (env.containsKey("APP_URL")) {
            base = env.get("APP_URL");
        } else {
            String mapping = DemoTools.run(DemoTools.command(DemoTools.compose(env), "port", "app", "8080"),
                    env, 5).check().output().strip();
            if (mapping.isEmpty()) return;
            base = "http://" + mapping.lines().findFirst().orElseThrow();
        }
        double cpu = Double.parseDouble(stats.get("CPUPerc").toString().replace("%", ""));
        double memory = Double.parseDouble(stats.get("MemPerc").toString().replace("%", ""));
        if (transport == null) transport = DemoTools.transport(env);
        var response = transport.request(base + "/resources?cpu_percent=" + cpu + "&memory_percent=" + memory,
                Map.of(), "POST", 2, DemoTools.postHeaders(env));
        if (response.status() >= 400) throw new IOException("Resource publication HTTP " + response.status());
    }

    public void annotateIntervals() throws IOException {
        var intervals = new ArrayList<Map<String, Object>>();
        String prefix = DemoTools.safeId(runId) + "-";
        try (var files = Files.list(directory.getParent())) {
            for (Path resultPath : files.filter(file -> file.getFileName().toString().startsWith(prefix)
                    && file.toString().endsWith(".json")).toList()) {
                try {
                    var result = DemoTools.map(LoadJson.parse(Files.readString(resultPath)));
                    for (String phase : List.of("warmup", "measure")) {
                        var report = DemoTools.map(result.get(phase));
                        if (!report.containsKey("start_epoch_seconds") || !report.containsKey("end_epoch_seconds")
                                || !report.containsKey("end_arrivals_epoch_seconds")) continue;
                        var interval = new LinkedHashMap<String, Object>();
                        interval.put("phase", phase);
                        for (String key : List.of("mode", "scenario", "level")) interval.put(key, result.get(key));
                        for (String key : List.of("start_epoch_seconds", "end_arrivals_epoch_seconds", "end_epoch_seconds"))
                            interval.put(key, report.get(key));
                        intervals.add(interval);
                    }
                } catch (IOException | IllegalArgumentException ignored) {}
            }
        }
        if (intervals.isEmpty() || !Files.exists(path)) return;
        Path staging = path.resolveSibling("resources.jsonl.partial");
        try (var source = Files.newBufferedReader(path); var destination = Files.newBufferedWriter(staging)) {
            String line;
            while ((line = source.readLine()) != null) {
                var record = new LinkedHashMap<>(DemoTools.map(LoadJson.parse(line)));
                double start = Instant.parse(record.get("timestamp").toString()).toEpochMilli() / 1000.0;
                double end = Instant.parse(record.getOrDefault("collected_at", record.get("timestamp")).toString())
                        .toEpochMilli() / 1000.0;
                var matching = new ArrayList<Map<String, Object>>();
                for (var interval : intervals) {
                    if (start <= number(interval, "end_epoch_seconds") && end >= number(interval, "start_epoch_seconds")) {
                        var annotated = new LinkedHashMap<>(interval);
                        annotated.put("arrival_window_overlap", start <= number(interval, "end_arrivals_epoch_seconds")
                                && end >= number(interval, "start_epoch_seconds"));
                        matching.add(annotated);
                    }
                }
                record.put("workload_intervals", matching);
                write(destination, record);
            }
        }
        Files.move(staging, path, StandardCopyOption.REPLACE_EXISTING);
    }

    private static double number(Map<String, Object> value, String key) { return ((Number) value.get(key)).doubleValue(); }
}

void main(String[] args) throws Exception {
    if (Arrays.asList(args).contains("--help")) {
        System.out.println("Usage: java load/Resources.java [Runner options]\nCollect Docker statistics during an HTAP comparison; --scenario htap --mode both are enforced.");
        return;
    }
    var env = System.getenv();
    String runId = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    for (int i = 0; i < args.length; i++) {
        if (args[i].equals("--run-id")) runId = args[++i];
        else if (args[i].startsWith("--run-id=")) runId = args[i].substring(9);
    }
    Path output = Path.of(env.getOrDefault("RESULTS_DIR", DemoTools.root().resolve("results").toString()));
    DemoTools.run(DemoTools.command(DemoTools.compose(env), "--profile", "tools", "build", "load"), env, 900).check();
    var sampler = new ResourceSampler(output, runId, env);
    String container = env.getOrDefault("COMPOSE_PROJECT_NAME", "ojp-demo") + "-resources-"
            + UUID.randomUUID().toString().substring(0, 8);
    var command = DemoTools.command(DemoTools.compose(env), "--profile", "tools", "run", "--rm",
            "--name", container, "--user", DemoTools.localUser(env), "load", "--output", "/results", "--run-id", runId);
    command.addAll(Arrays.asList(args));
    command.addAll(List.of("--scenario", "htap", "--mode", "both"));
    class Cleanup {
        volatile Process process;
        Exception failure;
        private final Object state = new Object();
        private boolean cleaning;
        private boolean finished;

        void shutdown(Thread owner) {
            synchronized (state) {
                if (!cleaning) owner.interrupt();
            }
            try { owner.join(10000); }
            catch (InterruptedException error) { Thread.currentThread().interrupt(); }
            run();
        }

        synchronized void run() {
            if (finished) return;
            boolean interrupted;
            synchronized (state) {
                cleaning = true;
                interrupted = Thread.interrupted();
            }
            try {
                try { if (process != null && process.isAlive()) DemoTools.terminate(process, 5); }
                catch (Exception error) { failure = error; System.err.println("Resource runner cleanup: " + error); }
                try { DemoTools.run(List.of("docker", "rm", "-f", container), env, 10); }
                catch (Exception error) { failure = error; System.err.println("Resource container cleanup: " + error); }
                try { sampler.stop(); }
                catch (Exception error) { failure = error; System.err.println("Resource sampler cleanup: " + error); }
                System.out.println("Container CPU/memory evidence: " + sampler.path);
            } finally {
                finished = true;
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
    }
    var cleanup = new Cleanup();
    Thread owner = Thread.currentThread();
    Thread shutdownHook = new Thread(() -> cleanup.shutdown(owner), "resource-cleanup");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    int exit;
    try {
        sampler.start();
        cleanup.process = new ProcessBuilder(command).directory(DemoTools.root().toFile()).inheritIO().start();
        exit = cleanup.process.waitFor();
    } finally {
        cleanup.run();
        try { Runtime.getRuntime().removeShutdownHook(shutdownHook); }
        catch (IllegalStateException ignored) {}
    }
    if (exit != 0) System.exit(exit);
    if (cleanup.failure != null) throw cleanup.failure;
}
