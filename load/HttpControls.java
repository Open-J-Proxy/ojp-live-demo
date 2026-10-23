import java.util.*;

void main(String[] args) throws Exception {
    if (Arrays.asList(args).contains("--help") || args.length == 0) {
        System.out.println("Usage: java load/HttpControls.java ready|reset|verify --url URL [--timeout SECONDS]");
        return;
    }
    String action = args[0], url = null;
    double timeout = 180;
    for (int i = 1; i < args.length; i++) {
        switch (args[i]) {
            case "--url" -> url = args[++i];
            case "--timeout" -> timeout = Double.parseDouble(args[++i]);
            default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
        }
    }
    if (url == null || !Set.of("ready", "reset", "verify").contains(action)
            || !Double.isFinite(timeout) || timeout <= 0) throw new IllegalArgumentException("Invalid command, URL or timeout");
    var env = System.getenv();
    if ("0".equals(env.get("APP_PORT")) && !env.containsKey("APP_URL")) url = DemoTools.applicationUrl(env);
    try (var transport = DemoTools.transport(env)) {
        if (action.equals("ready")) {
            long deadline = System.nanoTime() + (long) (timeout * 1e9);
            String last = "";
            while (System.nanoTime() < deadline) {
                try {
                    double remaining = (deadline - System.nanoTime()) / 1e9;
                    if (Boolean.TRUE.equals(DemoTools.map(call(transport, url, "/ready", false,
                            Math.max(.001, Math.min(5, remaining)), env)).get("ready"))) return;
                } catch (Exception error) { last = error.toString(); }
                Thread.sleep(500);
            }
            throw new IllegalStateException("Application readiness timed out: " + last);
        }
        if (action.equals("reset")) {
            System.out.println(LoadJson.stringify(call(transport, url, "/reset", true, 120, env)));
        } else {
            String mode = DemoTools.map(call(transport, url, "/ready", false, 120, env))
                    .getOrDefault("mode", "direct").toString();
            try {
                for (String selected : List.of("direct", "ojp")) {
                    call(transport, url, "/mode?value=" + selected, true, 120, env);
                    var result = DemoTools.map(call(transport, url,
                            "/work?op=browse&seed=12345&phase=warmup&scenario=integration&level=1", false, 120, env));
                    if (!Boolean.TRUE.equals(result.getOrDefault("sql_completed", result.getOrDefault("completed", false)))) {
                        throw new IllegalStateException(result.toString());
                    }
                    System.out.println(selected + ": SQL query succeeded");
                }
            } finally { call(transport, url, "/mode?value=" + mode, true, 120, env); }
        }
    }
}

Object call(DemoTools.Transport transport, String base, String path, boolean post, double timeout,
            Map<String, String> env) throws Exception {
    var response = transport.request(base + path, post ? Map.of() : null, post ? "POST" : "GET",
            timeout, post ? DemoTools.postHeaders(env) : Map.of());
    if (response.status() != 200) throw new IllegalStateException("HTTP " + response.status() + ": " + response.body());
    return response.body();
}
