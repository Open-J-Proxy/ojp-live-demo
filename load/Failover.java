import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

static final class Options {
    double recoveryTimeout = 60, warmup = 5, duration = 30;
    String runId = "failover-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    String scenario = "failover";
    final List<String> forwarded = new ArrayList<>();
}

Options options(String[] args) {
    var result = new Options();
    for (int i = 0; i < args.length; i++) {
        String option = args[i], value = null;
        int equals = option.indexOf('=');
        if (equals >= 0) { value = option.substring(equals + 1); option = option.substring(0, equals); }
        if (Set.of("--warmup-only", "--validate-only").contains(option))
            throw new IllegalArgumentException("Failover requires measurement; " + option + " is not supported");
        if (!Set.of("--recovery-timeout", "--warmup", "--duration", "--run-id", "--scenario", "--mode").contains(option)) {
            result.forwarded.add(args[i]);
            continue;
        }
        if (value == null) {
            if (++i >= args.length) throw new IllegalArgumentException("Missing value for " + option);
            value = args[i];
        }
        switch (option) {
            case "--recovery-timeout" -> result.recoveryTimeout = Double.parseDouble(value);
            case "--warmup" -> result.warmup = Double.parseDouble(value);
            case "--duration" -> result.duration = Double.parseDouble(value);
            case "--run-id" -> result.runId = value;
            case "--scenario" -> result.scenario = value;
            case "--mode" -> { if (!value.equals("ojp")) throw new IllegalArgumentException("Failover requires --mode ojp"); }
        }
    }
    if (!Double.isFinite(result.recoveryTimeout) || result.recoveryTimeout <= 0
            || !Double.isFinite(result.warmup) || result.warmup < 0
            || !Double.isFinite(result.duration) || result.duration < 3)
        throw new IllegalArgumentException("Recovery timeout must be positive, duration >= 3 and warmup >= 0");
    return result;
}

void main(String[] argv) throws Exception {
    if (Arrays.asList(argv).contains("--help") || Arrays.asList(argv).contains("-h")) {
        System.out.println("Usage: java load/Failover.java [--recovery-timeout 60] [--warmup 5] [--duration 30]"
                + " [--run-id ID] [--scenario failover] [Runner options]\nRequires three consecutive real SQL successes after stopping an HA node.");
        return;
    }
    var args = options(argv);
    var loadOptions = new ArrayList<>(List.of("--mode", "ojp", "--scenario", args.scenario,
            "--run-id", args.runId, "--warmup", Double.toString(args.warmup), "--duration", Double.toString(args.duration)));
    loadOptions.addAll(args.forwarded);
    Runner.arguments(loadOptions.toArray(String[]::new));
    Map<String, String> original = System.getenv();
    var env = new HashMap<>(original);
    env.put("OJP_URL", env.getOrDefault("OJP_HA_URL",
            "jdbc:ojp[ojp:1059,ojp2:1059,ojp3:1059]_postgresql://postgres:5432/shop?ApplicationName=demo-ojp"));
    Path output = Path.of(env.getOrDefault("RESULTS_DIR", DemoTools.root().resolve("results").toString()));
    Files.createDirectories(output);
    String safeId = DemoTools.safeId(args.runId);
    Path evidence = output.resolve(safeId + "-failover-events.json");
    Path pendingEvidence = output.resolve(safeId + "-failover-events.json.partial");
    Path logPath = output.resolve(safeId + "-failover-runner.log");
    if (Files.exists(evidence) || Files.exists(pendingEvidence) || Files.exists(logPath))
        throw new FileAlreadyExistsException("Failover evidence already exists; choose a new --run-id: " + args.runId);
    var sampler = new Resources.ResourceSampler(output, args.runId, env);
    Files.createFile(pendingEvidence);
    Files.createFile(logPath);
    var report = new LinkedHashMap<String, Object>();
    var observations = new ArrayList<Map<String, Object>>();
    var cleanupErrors = new ArrayList<Object>();
    report.put("run_id", args.runId); report.put("scenario", args.scenario); report.put("started_at", now());
    report.put("stopped_service", "ojp"); report.put("recovery_timeout_seconds", args.recoveryTimeout);
    report.put("observations", observations); report.put("transient_errors", 0);
    String container = env.getOrDefault("COMPOSE_PROJECT_NAME", "ojp-demo") + "-failover-"
            + UUID.randomUUID().toString().substring(0, 8);
    class Cleanup {
        volatile Process process;
        volatile DemoTools.Transport transport;
        volatile boolean shutdown;
        private final Object state = new Object();
        private boolean cleaning;
        private boolean finished;

        void shutdown(Thread owner) {
            synchronized (state) {
                shutdown = true;
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
                if (shutdown) {
                    report.put("success", false);
                    report.put("error", "Interrupted by JVM shutdown");
                }
                try { if (process != null && process.isAlive()) DemoTools.terminate(process, shutdown ? 2 : 15); }
                catch (Exception error) { cleanupErrors.add(error.toString()); }
                try { DemoTools.run(List.of("docker", "rm", "-f", container), env, shutdown ? 5 : 15); }
                catch (Exception error) { cleanupErrors.add(error.toString()); }
                try { sampler.stop(); }
                catch (Exception error) { cleanupErrors.add(error.toString()); }
                for (var command : List.of(new String[]{"start", "ojp"},
                        new String[]{"up", "-d", "--no-deps", "--force-recreate", "app"})) {
                    try {
                        var result = compose(original, shutdown ? 20 : 180, command);
                        if (result.exitCode() != 0) cleanupErrors.add(result.exitCode());
                    } catch (Exception error) { cleanupErrors.add(error.toString()); }
                }
                if (cleanupErrors.isEmpty()) {
                    try {
                        if (transport == null) transport = DemoTools.transport(original);
                        ready(transport, DemoTools.applicationUrl(original), shutdown ? 10 : 180);
                    } catch (Exception error) { cleanupErrors.add(error.toString()); }
                }
                try { if (transport != null) transport.close(); }
                catch (Exception error) { cleanupErrors.add(error.toString()); }
                if (shutdown) {
                    report.put("success", false);
                    report.put("error", "Interrupted by JVM shutdown");
                }
                if (!cleanupErrors.isEmpty()) report.put("cleanup_errors", cleanupErrors);
                report.put("finished_at", now());
                try {
                    Files.writeString(pendingEvidence, LoadJson.stringify(report) + "\n");
                    // Publish the complete file atomically without replacing another run's evidence.
                    Files.createLink(evidence, pendingEvidence);
                    Files.delete(pendingEvidence);
                }
                catch (Exception error) {
                    cleanupErrors.add(error.toString());
                    System.err.println("Failover evidence: " + error);
                }
                System.out.println(evidence);
            } finally {
                finished = true;
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
    }
    var cleanup = new Cleanup();
    Thread owner = Thread.currentThread();
    Thread shutdownHook = new Thread(() -> cleanup.shutdown(owner), "failover-cleanup");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    try {
        compose(env, 300, "--profile", "ha", "up", "-d", "ojp", "ojp2", "ojp3").check();
        compose(env, 300, "up", "-d", "--no-deps", "--force-recreate", "app").check();
        cleanup.transport = DemoTools.transport(env);
        var transport = cleanup.transport;
        String base = DemoTools.applicationUrl(env);
        ready(transport, base, 180);
        sampler.start();
        var runner = DemoTools.command(DemoTools.compose(env), "--profile", "tools", "run", "--rm",
                "--name", container, "--user", DemoTools.localUser(env), "load", "--output", "/results");
        runner.addAll(loadOptions);
        cleanup.process = DemoTools.start(runner, env, logPath);
        var process = cleanup.process;
        long deadline = deadline(args.warmup + 180);
        boolean measuring = false;
        while (remaining(deadline) > 0) {
            if (!process.isAlive()) throw new IOException("Load runner exited before measurement; see " + logPath);
            var health = DemoTools.map(http(transport, base, "/health", Math.min(5, remaining(deadline))).body());
            var context = DemoTools.map(health.getOrDefault("context", health));
            measuring = "measure".equals(context.get("phase")) && args.runId.equals(context.get("run_id"));
            if (!measuring) {
                Object metrics = http(transport, base, "/metrics", Math.max(.001, Math.min(5, remaining(deadline)))).body();
                if (metrics instanceof String text) measuring = measured(text, args.scenario);
            }
            if (measuring) { report.put("measurement_observed_at", now()); break; }
            Thread.sleep(200);
        }
        if (!measuring) throw new IOException("Never observed this run's measurement context");
        var nodes = new ArrayList<Map<String, Object>>();
        for (String line : compose(env, 30, "ps", "--format", "json", "ojp", "ojp2", "ojp3").check().output().lines().toList()) {
            if (line.isBlank()) continue;
            Object parsed = LoadJson.parse(line);
            if (parsed instanceof List<?> list) list.forEach(node -> nodes.add(DemoTools.map(node)));
            else nodes.add(DemoTools.map(parsed));
        }
        var nodeReports = new ArrayList<Map<String, Object>>();
        var services = new HashSet<String>();
        for (var node : nodes) {
            var value = new LinkedHashMap<String, Object>();
            value.put("service", node.get("Service")); value.put("state", node.get("State")); value.put("health", node.get("Health"));
            nodeReports.add(value); services.add(node.get("Service").toString());
        }
        report.put("ha_nodes_before_fault", nodeReports);
        if (!services.equals(Set.of("ojp", "ojp2", "ojp3"))
                || nodes.stream().anyMatch(node -> !"running".equals(node.get("State"))))
            throw new IOException("All three HA nodes must be running before fault injection");
        Thread.sleep((long) (Math.min(2, args.duration / 3) * 1000));
        if (!process.isAlive()) throw new IOException("Measurement ended before fault injection");
        report.put("fault_requested_at", now());
        long fault = System.nanoTime();
        long recoveryDeadline = deadline(args.recoveryTimeout);
        compose(env, Math.max(.001, remaining(recoveryDeadline)), "stop", "-t", "0", "ojp").check();
        report.put("node_stopped_at", now());
        int consecutive = 0, transientErrors = 0;
        while (remaining(recoveryDeadline) > 0) {
            var response = http(transport, base, "/work?op=browse&seed=12345&phase=measure&scenario="
                    + URLEncoder.encode(args.scenario, StandardCharsets.UTF_8) + "&level=0",
                    Math.max(.001, Math.min(5, remaining(recoveryDeadline))));
            var body = DemoTools.map(response.body());
            boolean success = response.status() == 200
                    && Boolean.TRUE.equals(body.getOrDefault("ok", body.getOrDefault("success", false)))
                    && Boolean.TRUE.equals(body.getOrDefault("sql_completed", body.getOrDefault("completed", false)))
                    && !Boolean.TRUE.equals(body.getOrDefault("rolledBack", body.getOrDefault("rollback", false)));
            observations.add(Map.of("timestamp", now(), "status", response.status(), "sql_success", success,
                    "response", response.body() == null ? "" : response.body()));
            if (success) {
                if (++consecutive >= 3) {
                    report.put("recovered_at", now()); report.put("recovery_seconds", (System.nanoTime() - fault) / 1e9);
                    break;
                }
            } else { consecutive = 0; report.put("transient_errors", ++transientErrors); }
            Thread.sleep(200);
        }
        if (consecutive < 3) throw new IOException("No three consecutive SQL successes within recovery bound");
        if (!process.waitFor((long) Math.ceil(args.duration + args.warmup + args.recoveryTimeout + 180), TimeUnit.SECONDS))
            throw new IOException("Load runner timed out; see " + logPath);
        report.put("runner_exit_code", process.exitValue());
        if (process.exitValue() != 0) throw new IOException("Load runner failed; see " + logPath);
        report.put("success", true);
    } catch (Exception error) {
        report.put("success", false); report.put("error", error.toString());
    } finally {
        cleanup.run();
        try { Runtime.getRuntime().removeShutdownHook(shutdownHook); }
        catch (IllegalStateException ignored) {}
    }
    if (!Boolean.TRUE.equals(report.get("success")) || !cleanupErrors.isEmpty())
        throw new IllegalStateException(report.getOrDefault("error", "HA cleanup failed").toString());
}

DemoTools.Result compose(Map<String, String> env, double timeout, String... args) throws Exception {
    return DemoTools.run(DemoTools.command(DemoTools.compose(env), args), env, timeout);
}

String now() { return Instant.now().toString(); }
long deadline(double seconds) { return System.nanoTime() + (long) (seconds * 1e9); }
double remaining(long deadline) { return (deadline - System.nanoTime()) / 1e9; }

DemoTools.Response http(DemoTools.Transport transport, String base, String path, double timeout) {
    try { return transport.request(base + path, null, "GET", timeout, Map.of()); }
    catch (Exception error) {
        if (error instanceof InterruptedException) Thread.currentThread().interrupt();
        return new DemoTools.Response(0, error.toString());
    }
}

void ready(DemoTools.Transport transport, String base, double timeout) throws Exception {
    long deadline = deadline(timeout);
    while (remaining(deadline) > 0) {
        if (http(transport, base, "/ready", Math.max(.001, Math.min(5, remaining(deadline)))).status() == 200) return;
        Thread.sleep(500);
    }
    throw new IOException("Application did not become ready after HA reconfiguration");
}

boolean measured(String metrics, String scenario) {
    for (String line : metrics.lines().toList()) {
        if (!line.startsWith("demo_completions_total{")) continue;
        var labels = new HashMap<String, String>();
        var matches = Pattern.compile("(\\w+)=\"([^\"]*)\"").matcher(line);
        while (matches.find()) labels.put(matches.group(1), matches.group(2));
        if ("measure".equals(labels.get("phase")) && scenario.equals(labels.get("scenario"))) {
            try { if (Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)) > 0) return true; }
            catch (NumberFormatException ignored) {}
        }
    }
    return false;
}
