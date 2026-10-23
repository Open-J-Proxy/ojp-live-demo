import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

// Java 25 compact source file (JEP 512); run with: java Runner.java [options].
static final Map<String, Integer> OPERATIONS = operationDefaults();
static final double[] LATENCY_BUCKETS = {.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60};
static final String[] BUCKET_NAMES = {"0.005", "0.01", "0.025", "0.05", "0.1", "0.25",
        "0.5", "1", "2.5", "5", "10", "30", "60", "+Inf"};
static final DateTimeFormatter TIMESTAMP = new DateTimeFormatterBuilder().appendInstant(3).toFormatter();

static Map<String, Integer> operationDefaults() {
    Map<String, Integer> result = new LinkedHashMap<>();
    result.put("browse", 30);
    result.put("history", 25);
    result.put("checkout", 20);
    result.put("review", 10);
    result.put("account", 5);
    result.put("report", 10);
    return Collections.unmodifiableMap(result);
}

static class Args {
    String url = "http://app:8080", mode = "both", scenario = "htap";
    List<Integer> rps = List.of(8, 256, 8, 8);
    double warmup = 10, duration = 30, requestTimeout = 30, controlTimeout = 210, readinessTimeout = 90;
    int maxInFlight = 512, oltpPercent = 98, olapPercent = 2;
    BigInteger seed = BigInteger.valueOf(12345);
    boolean warmupOnly, validateOnly;
    String runId = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    String ojpMetricsUrl = "http://ojp:9159/metrics";
    Map<String, Integer> operationMix;
    Path output = Path.of(System.getenv().getOrDefault("RESULTS_DIR",
            Files.isDirectory(Path.of("/results")) ? "/results" : "results"));
}

static class HelpRequested extends RuntimeException {}

static double number(String value, boolean zeroAllowed) {
    String decimal = value.strip();
    if (!decimal.matches("[+-]?(?:[0-9](?:_?[0-9])*(?:\\.(?:[0-9](?:_?[0-9])*)?)?"
            + "|\\.[0-9](?:_?[0-9])*)(?:[eE][+-]?[0-9](?:_?[0-9])*)?"))
        throw new IllegalArgumentException("expected a decimal number");
    double result = Double.parseDouble(decimal.replace("_", ""));
    if (!Double.isFinite(result) || (zeroAllowed ? result < 0 : result <= 0))
        throw new IllegalArgumentException(zeroAllowed
                ? "must be finite and nonnegative" : "must be finite and greater than zero");
    return result;
}

static int integer(String value) {
    String decimal = value.strip();
    if (!decimal.matches("[+-]?[0-9](?:_?[0-9])*"))
        throw new IllegalArgumentException("expected an integer");
    return Integer.parseInt(decimal.replace("_", ""));
}

static int positiveInt(String value) {
    int result = integer(value);
    if (result <= 0) throw new IllegalArgumentException("must be greater than zero");
    return result;
}

static int percentage(String value) {
    int result = integer(value);
    if (result < 0 || result > 100) throw new IllegalArgumentException("must be an integer from 0 to 100");
    return result;
}

static List<Integer> rates(String value) {
    List<Integer> result = new ArrayList<>();
    for (String part : value.split(",", -1)) {
        int rate = positiveInt(part.strip());
        if (rate > 10000) throw new IllegalArgumentException("RPS must not exceed 10000");
        result.add(rate);
    }
    if (new LinkedHashSet<>(result).size() > 64)
        throw new IllegalArgumentException("at most 64 distinct RPS levels are supported");
    return result;
}

static Map<String, Integer> operationMix(String value) {
    Map<String, Integer> weights = new LinkedHashMap<>();
    for (String entry : value.split(",", -1)) {
        String[] pair = entry.split("=", -1);
        if (pair.length != 2) throw new IllegalArgumentException("expected operation=weight");
        String name = pair[0].strip();
        int weight = integer(pair[1].strip());
        if (!OPERATIONS.containsKey(name) || weights.containsKey(name) || weight < 0)
            throw new IllegalArgumentException("unknown, duplicate or negative operation weight");
        weights.put(name, weight);
    }
    if (!weights.keySet().equals(OPERATIONS.keySet())
            || weights.values().stream().mapToLong(Integer::longValue).sum() != 100)
        throw new IllegalArgumentException("all six operation weights are required and must total 100");
    return weights;
}

static Args arguments(String[] argv) {
    Args args = new Args();
    for (int i = 0; i < argv.length; i++) {
        String option = argv[i], value = null;
        int equals = option.indexOf('=');
        if (equals >= 0) { value = option.substring(equals + 1); option = option.substring(0, equals); }
        if ((option.equals("--help") || option.equals("-h")) && value == null) throw new HelpRequested();
        if (option.equals("--warmup-only") || option.equals("--validate-only")) {
            if (value != null) throw new IllegalArgumentException(option + " does not take a value");
            if (option.equals("--warmup-only")) args.warmupOnly = true;
            else args.validateOnly = true;
            continue;
        }
        if (value == null) {
            if (++i >= argv.length) throw new IllegalArgumentException("expected a value for " + option);
            value = argv[i];
            if (value.startsWith("--") || value.equals("-h"))
                throw new IllegalArgumentException("expected a value for " + option);
        }
        try {
            switch (option) {
                case "--url" -> args.url = value;
                case "--mode" -> args.mode = choice(value, "direct", "ojp", "both");
                case "--rps" -> args.rps = rates(value);
                case "--warmup" -> args.warmup = number(value, true);
                case "--duration" -> args.duration = number(value, false);
                case "--max-in-flight" -> args.maxInFlight = positiveInt(value);
                case "--request-timeout" -> args.requestTimeout = number(value, false);
                case "--control-timeout" -> args.controlTimeout = number(value, false);
                case "--readiness-timeout" -> args.readinessTimeout = number(value, false);
                case "--seed" -> {
                    String decimal = value.strip();
                    if (!decimal.matches("[+-]?[0-9](?:_?[0-9])*"))
                        throw new IllegalArgumentException("expected an integer");
                    args.seed = new BigInteger(decimal.replace("_", ""));
                }
                case "--run-id" -> args.runId = value;
                case "--scenario" -> args.scenario = choice(value, "htap", "failover", "integration");
                case "--ojp-metrics-url" -> args.ojpMetricsUrl = value;
                case "--oltp-percent" -> args.oltpPercent = percentage(value);
                case "--olap-percent" -> args.olapPercent = percentage(value);
                case "--operation-mix" -> args.operationMix = operationMix(value);
                case "--output" -> args.output = Path.of(value);
                default -> throw new IllegalArgumentException("unrecognized argument: " + option);
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(option + ": " + e.getMessage(), e);
        }
    }
    if (args.oltpPercent + args.olapPercent != 100)
        throw new IllegalArgumentException("--oltp-percent and --olap-percent must total 100");
    if (args.operationMix != null && (args.operationMix.get("report") != args.olapPercent
            || args.operationMix.entrySet().stream().filter(e -> !e.getKey().equals("report"))
                    .mapToInt(Map.Entry::getValue).sum() != args.oltpPercent))
        throw new IllegalArgumentException("--operation-mix report/other weights must match --olap-percent/--oltp-percent");
    URI url = URI.create(args.url);
    if (!(Set.of("http", "https").contains(url.getScheme() == null ? "" : url.getScheme()))
            || url.getRawAuthority() == null || url.getHost() == null)
        throw new IllegalArgumentException("--url must be an absolute http(s) URL");
    if (args.runId.isEmpty() || args.runId.codePointCount(0, args.runId.length()) > 64)
        throw new IllegalArgumentException("--run-id must contain 1 to 64 characters");
    return args;
}

static String choice(String value, String... allowed) {
    if (!List.of(allowed).contains(value)) throw new IllegalArgumentException("invalid choice: " + value);
    return value;
}

static void help() {
    System.out.println("""
            Dependency-free, bounded open-loop HTTP workload runner.
            Usage: java Runner.java [options]
              --url URL                 application base URL (default: http://app:8080)
              --mode direct|ojp|both     connection paths, sequentially (default: both)
              --rps CSV                  ordered positive integer rates, max 10000/64 distinct
                                         (default: 8,256,8,8; baseline, spike, two post-spike windows)
              --warmup SECONDS           fully drained arrival window (default: 10; zero allowed)
              --warmup-only              reset, readiness, warm up and drain; never measure
              --validate-only            validate options without requests or result files
              --duration SECONDS         measurement arrivals excluding drain (default: 30)
              --max-in-flight INTEGER    maximum outstanding requests (default: 512)
              --request-timeout SECONDS  measured SQL timeout including body (default: 30)
              --control-timeout SECONDS  mode/reset/telemetry timeout (default: 210)
              --readiness-timeout SECONDS readiness polling deadline (default: 90)
              --seed INTEGER             reproducible work seed (default: 12345)
              --run-id ID                unique result ID, 1 to 64 characters
              --scenario htap|failover|integration (default: htap)
              --ojp-metrics-url URL      OJP Prometheus endpoint (default: http://ojp:9159/metrics)
              --oltp-percent INTEGER    integer OLTP share, rescales 30:25:20:10:5 (default: 98)
              --olap-percent INTEGER    integer report share; total must be 100 (default: 2)
              --operation-mix MIX       all six integer weights totaling 100, zeros allowed:
                                         browse=30,history=25,checkout=20,review=10,account=5,report=10
                                         report/others must match --olap-percent/--oltp-percent
              --output DIRECTORY        RESULTS_DIR, mounted /results, or local results
              -h, --help                show this help

            Each mode/rate resets, warms up, drains, resets, then measures.
            Arrivals never wait for capacity: excess work is recorded as dropped.
            HTTP 202 is not SQL completion. Optional ADMIN_TOKEN authenticates control POSTs.
            Docker build context is the repository root.
            """);
}

static int gcd(int a, int b) {
    while (b != 0) { int next = a % b; a = b; b = next; }
    return a;
}

static Map<String, Integer> operationWeights() { return operationWeights(90, 10); }
static Map<String, Integer> operationWeights(int oltp, int olap) {
    Map<String, Integer> weights = new LinkedHashMap<>();
    for (var entry : OPERATIONS.entrySet())
        weights.put(entry.getKey(), entry.getKey().equals("report") ? olap * 90 : entry.getValue() * oltp);
    int divisor = 0;
    for (int weight : weights.values()) divisor = gcd(divisor, weight);
    final int common = divisor;
    weights.replaceAll((op, weight) -> weight / common);
    return weights;
}

// Match Python's integer-seeded MT19937, shuffle and getrandbits. Existing seeds
// therefore replay the same plan, including arbitrary-size and negative seeds.
static class WorkRandom {
    final int[] state = new int[624];
    int index = 624;

    WorkRandom(BigInteger seed) {
        BigInteger absolute = seed.abs();
        int[] key = new int[Math.max(1, (absolute.bitLength() + 31) / 32)];
        for (int i = 0; i < key.length; i++) key[i] = absolute.shiftRight(i * 32).intValue();
        state[0] = 19650218;
        for (int i = 1; i < 624; i++)
            state[i] = 1812433253 * (state[i - 1] ^ (state[i - 1] >>> 30)) + i;
        int i = 1, j = 0;
        for (int k = Math.max(624, key.length); k > 0; k--) {
            state[i] = (state[i] ^ ((state[i - 1] ^ (state[i - 1] >>> 30)) * 1664525)) + key[j] + j;
            if (++i >= 624) { state[0] = state[623]; i = 1; }
            if (++j >= key.length) j = 0;
        }
        for (int k = 623; k > 0; k--) {
            state[i] = (state[i] ^ ((state[i - 1] ^ (state[i - 1] >>> 30)) * 1566083941)) - i;
            if (++i >= 624) { state[0] = state[623]; i = 1; }
        }
        state[0] = 0x80000000;
    }

    int nextWord() {
        if (index >= 624) {
            for (int i = 0; i < 624; i++) {
                int y = (state[i] & 0x80000000) | (state[(i + 1) % 624] & 0x7fffffff);
                state[i] = state[(i + 397) % 624] ^ (y >>> 1) ^ ((y & 1) == 0 ? 0 : 0x9908b0df);
            }
            index = 0;
        }
        int y = state[index++];
        y ^= y >>> 11;
        y ^= (y << 7) & 0x9d2c5680;
        y ^= (y << 15) & 0xefc60000;
        y ^= y >>> 18;
        return y;
    }

    long workSeed() {
        long low = Integer.toUnsignedLong(nextWord());
        long high = Integer.toUnsignedLong(nextWord()) >>> 1;
        return low | (high << 32);
    }

    int below(int bound) {
        int bits = 32 - Integer.numberOfLeadingZeros(bound);
        int result;
        do { result = nextWord() >>> (32 - bits); } while (result >= bound);
        return result;
    }

    void shuffle(List<String> block) {
        for (int i = block.size() - 1; i > 0; i--) Collections.swap(block, i, below(i + 1));
    }
}

static Iterator<String> operations(long seed, Map<String, Integer> weights) {
    return operations(BigInteger.valueOf(seed), weights);
}
static Iterator<String> operations(BigInteger seed, Map<String, Integer> weights) {
    List<String> block = new ArrayList<>();
    weights.forEach((op, weight) -> { for (int i = 0; i < weight; i++) block.add(op); });
    if (block.isEmpty()) throw new IllegalArgumentException("empty operation weights");
    return new Iterator<>() {
        final WorkRandom random = new WorkRandom(seed);
        int index = block.size();
        public boolean hasNext() { return true; }
        public String next() {
            if (index == block.size()) { random.shuffle(block); index = 0; }
            return block.get(index++);
        }
    };
}

static Map<String, Integer> weights(Args args) {
    return args.operationMix != null ? args.operationMix : operationWeights(args.oltpPercent, args.olapPercent);
}

static Map<String, Object> map(Object... pairs) {
    Map<String, Object> result = new LinkedHashMap<>();
    for (int i = 0; i < pairs.length; i += 2) result.put((String) pairs[i], pairs[i + 1]);
    return result;
}

@SuppressWarnings("unchecked")
static Map<String, Object> object(Object value) {
    return value instanceof Map<?, ?> ? (Map<String, Object>) value : Collections.emptyMap();
}

static boolean truth(Object value) {
    return value != null && !Boolean.FALSE.equals(value)
            && (!(value instanceof Number number) || number.doubleValue() != 0)
            && (!(value instanceof String string) || !string.isEmpty())
            && (!(value instanceof Collection<?> collection) || !collection.isEmpty())
            && (!(value instanceof Map<?, ?> map) || !map.isEmpty());
}

record Response(int status, Object data, String body) {}
@FunctionalInterface
interface Transport {
    Response request(String base, String path, double timeout, String method, Map<String, ?> query) throws Exception;
}
static volatile Transport transport = (base, path, timeout, method, query) ->
        httpRequest(base, path, timeout, method, query);

static class Http {
    static final HttpClient CLIENT = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
}

static Duration timeoutDuration(double seconds) {
    return Duration.ofNanos(Math.max(1, (long) Math.min(Long.MAX_VALUE, seconds * 1_000_000_000.0)));
}

static Response httpRequest(String base, String path, double timeout, String method,
                            Map<String, ?> query) throws Exception {
    StringBuilder url = new StringBuilder(base.replaceAll("/+$", "")).append(path);
    if (query != null && !query.isEmpty()) {
        url.append('?');
        boolean first = true;
        for (var entry : query.entrySet()) {
            if (!first) url.append('&');
            first = false;
            url.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)).append('=')
                    .append(URLEncoder.encode(String.valueOf(entry.getValue()), StandardCharsets.UTF_8));
        }
    }
    HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url.toString()))
            .timeout(timeoutDuration(timeout));
    if (method.equals("POST")) {
        builder.POST(HttpRequest.BodyPublishers.noBody());
        String token = System.getenv("ADMIN_TOKEN");
        if (token != null && !token.isEmpty()) builder.header("Authorization", "Bearer " + token);
    } else {
        builder.GET();
    }
    // HttpRequest.timeout alone is not a body deadline. Await the complete body
    // with a separate deadline and cancel the exchange on timeout/interruption.
    CompletableFuture<HttpResponse<String>> future = Http.CLIENT.sendAsync(builder.build(),
            HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    HttpResponse<String> response;
    try {
        response = future.get(timeoutDuration(timeout).toNanos(), TimeUnit.NANOSECONDS);
    } catch (TimeoutException e) {
        future.cancel(true);
        throw new HttpTimeoutException("HTTP request including body timed out");
    } catch (InterruptedException e) {
        future.cancel(true);
        Thread.currentThread().interrupt();
        throw e;
    } catch (ExecutionException e) {
        if (e.getCause() instanceof Exception cause) throw cause;
        throw new IOException("HTTP exchange failed", e.getCause());
    }
    Object data;
    try { data = LoadJson.parse(response.body()); }
    catch (IllegalArgumentException e) { data = null; }
    return new Response(response.statusCode(), data, response.body());
}

static Response request(String base, String path, double timeout, String method,
                        Map<String, ?> query) throws Exception {
    return transport.request(base, path, timeout, method, query);
}

static Response request(String base, String path, double timeout) throws Exception {
    return request(base, path, timeout, "GET", null);
}

static Object control(Args args, String path, Map<String, ?> query, double timeout) throws Exception {
    Response response = request(args.url, path, timeout, "POST", query);
    if (response.status != 200 || Boolean.FALSE.equals(object(response.data).get("success")))
        throw new IllegalStateException(path + " failed: HTTP " + response.status
                + ", response=" + LoadJson.stringify(response.data));
    return response.data;
}
static Object control(Args args, String path, Map<String, ?> query) throws Exception {
    return control(args, path, query, args.controlTimeout);
}
static Object control(Args args, String path) throws Exception { return control(args, path, null); }

static boolean isReady(int status, Object data) {
    if (status != 200 || !(data instanceof Map<?, ?>)) return false;
    Map<String, Object> response = object(data);
    if (response.containsKey("ready")) return Boolean.TRUE.equals(response.get("ready"));
    if (response.containsKey("healthy")) return Boolean.TRUE.equals(response.get("healthy"));
    return response.get("status") instanceof String statusText
            && List.of("ready", "ok", "healthy", "UP", "up").contains(statusText);
}

static double monotonic() { return System.nanoTime() / 1_000_000_000.0; }
static double wallTime() { return Instant.now().toEpochMilli() / 1000.0; }
static void sleep(double seconds) throws InterruptedException {
    if (seconds > 0) TimeUnit.NANOSECONDS.sleep(timeoutDuration(seconds).toNanos());
}

static Map<String, Object> work(Args args, String op, long seed, String phase, String level) {
    return work(args, op, seed, phase, level, args.requestTimeout);
}
static Map<String, Object> work(Args args, String op, long seed, String phase, String level, double timeout) {
    return work(args, op, BigInteger.valueOf(seed), phase, level, timeout);
}
static Map<String, Object> work(Args args, String op, BigInteger seed, String phase, String level, double timeout) {
    double started = monotonic();
    Map<String, Object> result = map("accepted", false, "sql_completed", false,
            "successful_transactions", false, "timeouts", false, "error", null);
    try {
        Response response = request(args.url, "/work", timeout, "GET",
                map("op", op, "seed", seed, "phase", phase, "scenario", args.scenario, "level", level));
        Map<String, Object> data = object(response.data);
        result.put("accepted", data.containsKey("accepted") ? Boolean.TRUE.equals(data.get("accepted"))
                : response.status >= 200 && response.status < 300);
        if (response.status != 200) result.put("error", "http_" + response.status);
        else if (!(response.data instanceof Map<?, ?>)) result.put("error", "invalid_json");
        else if (!op.equals(data.getOrDefault("operation", data.get("op")))) result.put("error", "operation_mismatch");
        else if (!Boolean.TRUE.equals(data.get("success"))) result.put("error", "sql_failed");
        else if (!truth(data.getOrDefault("sql_completed", true))) result.put("error", "sql_not_completed");
        else {
            result.put("sql_completed", true);
            result.put("successful_transactions", !truth(data.getOrDefault("rollback", false)));
            if (!truth(result.get("successful_transactions"))) result.put("error", "rollback");
        }
    } catch (Exception e) {
        boolean timedOut = e instanceof HttpTimeoutException || e instanceof TimeoutException
                || e instanceof java.net.SocketTimeoutException;
        result.put("timeouts", timedOut);
        result.put("error", timedOut ? "timeout" : e instanceof IOException ? "transport_error"
                : e.getClass().getSimpleName());
    }
    result.put("started", started);
    result.put("completed", monotonic());
    return result;
}

static Map<String, Object> readiness(Args args, String level) throws Exception {
    double deadline = monotonic() + args.readinessTimeout;
    Map<String, Object> last = new LinkedHashMap<>();
    while (monotonic() < deadline) {
        try {
            boolean ready = true;
            for (String path : List.of("/health", "/ready")) {
                Response response = request(args.url, path,
                        Math.min(args.controlTimeout, Math.max(.001, deadline - monotonic())));
                last.put(path, map("status", response.status, "response", response.data));
                if (!isReady(response.status, response.data)) { ready = false; break; }
            }
            if (ready) {
                String op = weights(args).entrySet().stream().filter(e -> e.getValue() > 0)
                        .findFirst().orElseThrow().getKey();
                Map<String, Object> probe = work(args, op, args.seed, "warmup", level,
                        Math.min(args.controlTimeout, Math.max(.001, deadline - monotonic())));
                last.put("work_probe", probe);
                if (truth(probe.get("successful_transactions"))) return last;
            }
        } catch (Exception e) { last.put("error", e.toString()); }
        sleep(Math.min(.2, Math.max(0, deadline - monotonic())));
    }
    throw new IllegalStateException("readiness or real-work probe timed out: " + LoadJson.stringify(last));
}

static Map<String, Object> percentiles(Collection<Double> samples) {
    List<Double> ordered = new ArrayList<>(samples);
    ordered.sort(Double::compare);
    Map<String, Object> result = new LinkedHashMap<>();
    String[] names = {"p50", "p90", "p95", "p99"};
    double[] fractions = {.5, .9, .95, .99};
    for (int i = 0; i < names.length; i++)
        result.put(names[i], ordered.isEmpty() ? null
                : ordered.get(Math.max(0, (int) Math.ceil(ordered.size() * fractions[i]) - 1)) * 1000);
    result.put("max", ordered.isEmpty() ? null : ordered.getLast() * 1000);
    return result;
}

static String utcTimestamp(double seconds) {
    long millis = Math.round(seconds * 1000);
    return TIMESTAMP.format(Instant.ofEpochMilli(millis));
}

static Map<String, Object> counters(String... names) {
    Map<String, Object> result = new LinkedHashMap<>();
    for (String name : names) result.put(name, 0L);
    return result;
}
static long count(Map<String, Object> values, String key) {
    return ((Number) values.getOrDefault(key, 0L)).longValue();
}
static void increment(Map<String, Object> values, String key) { increment(values, key, 1); }
static void increment(Map<String, Object> values, String key, long value) {
    values.put(key, count(values, key) + value);
}
static double numeric(Object value) { return ((Number) value).doubleValue(); }

static class Phase {
    final Args args;
    final int rps;
    final double duration;
    final String phase, level, mode;
    final Object lock = new Object();
    final Map<String, Object> counts = counters("scheduled", "dispatched", "accepted", "sql_completed",
            "successful_transactions", "failed_transactions", "timeouts", "dropped", "in_flight",
            "peak_in_flight", "completed", "arrival_window_completed", "arrival_window_sql_completed",
            "arrival_window_successful_transactions");
    final Map<String, Object> perOp = new LinkedHashMap<>(), errors = new LinkedHashMap<>();
    final Map<String, List<Double>> endToEnd = new LinkedHashMap<>(), service = new LinkedHashMap<>();
    final long[] histogram = new long[LATENCY_BUCKETS.length + 1];
    final Map<String, Object> telemetry = map("samples_sent", 0L, "errors", new LinkedHashMap<String, Object>(),
            "final_sent", false);
    final List<Double> cpuSamples = new ArrayList<>(), cpuTimes = new ArrayList<>(),
            connectionSamples = new ArrayList<>(), connectionTimes = new ArrayList<>();
    volatile double start, wallStart;
    double latencySum;
    final Object stop = new Object();
    volatile boolean stopped;

    Phase(Args args, int rps, double duration, String phase, String level, String mode) {
        this.args = args; this.rps = rps; this.duration = duration;
        this.phase = phase; this.level = level; this.mode = mode;
        for (String op : OPERATIONS.keySet()) {
            perOp.put(op, counters("scheduled", "completed", "dispatched", "dropped",
                    "sql_completed", "successful_transactions"));
            endToEnd.put(op, new ArrayList<>());
            service.put(op, new ArrayList<>());
        }
        start = monotonic(); wallStart = wallTime();
    }

    void publish(boolean last) {
        Map<String, Object> payload = new LinkedHashMap<>();
        synchronized (lock) {
            for (String key : List.of("scheduled", "dispatched", "dropped", "accepted", "sql_completed",
                    "successful_transactions", "failed_transactions", "timeouts", "completed"))
                payload.put(key, counts.get(key));
            StringBuilder buckets = new StringBuilder();
            long total = 0;
            for (long bucket : histogram) {
                if (!buckets.isEmpty()) buckets.append(',');
                buckets.append(total += bucket);
            }
            payload.putAll(map("inflight", counts.get("in_flight"), "phase", phase, "level", level,
                    "mode", mode, "errors", counts.get("failed_transactions"),
                    "completed", counts.get("successful_transactions"), "http_completed", counts.get("completed"),
                    "reset", String.valueOf(count(telemetry, "samples_sent") == 0),
                    "scenario", args.scenario, "run_id", args.runId, "elapsed_seconds", monotonic() - start,
                    "final", String.valueOf(last), "latency_count", counts.get("completed"),
                    "latency_sum_seconds", latencySum, "latency_bucket_counts", buckets.toString()));
        }
        try {
            control(args, "/arrivals", payload, Math.min(args.controlTimeout, 10));
            increment(telemetry, "samples_sent");
            telemetry.put("final_sent", last);
        } catch (Exception e) {
            increment(object(telemetry.get("errors")), e.getClass().getSimpleName());
            if (last) System.err.println("WARNING: final /arrivals publication failed for phase=" + phase
                    + " level=" + level + " (" + e.getClass().getSimpleName() + "); metrics may be stale");
        }
    }

    void periodically() {
        while (true) {
            synchronized (stop) {
                if (stopped) return;
                try { stop.wait(1000); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                if (stopped) return;
            }
            publish(false);
            if (!phase.equals("measure")) continue;
            try {
                double timeout = Math.min(args.controlTimeout, 10);
                Response ready = request(args.url, "/ready", timeout);
                Response metrics = request(args.url, "/metrics", timeout);
                if (metrics.status == 200) {
                    Double cpu = metricValue(metrics.body, "demo_postgres_cpu_percent");
                    Double at = metricValue(metrics.body, "demo_postgres_resource_timestamp_seconds");
                    if (cpu != null && at != null && wallStart <= at && at <= wallStart + duration) {
                        cpuSamples.add(cpu); cpuTimes.add(at);
                    }
                }
                if (ready.status == 200) {
                    Map<String, Object> data = object(ready.data);
                    Object at = data.get("postgres_snapshot_timestamp_seconds");
                    Object physical = data.get(mode.equals("direct") ? "direct_sessions" : "ojp_sessions");
                    if (at instanceof Number && physical instanceof Number
                            && numeric(physical) >= 0 && wallStart <= numeric(at)
                            && numeric(at) <= wallStart + duration) {
                        connectionSamples.add(numeric(physical)); connectionTimes.add(numeric(at));
                    }
                }
            } catch (Exception ignored) { }
        }
    }

    void complete(String op, double scheduled, Map<String, Object> result) {
        synchronized (lock) {
            try {
                for (String key : List.of("accepted", "sql_completed", "successful_transactions", "timeouts"))
                    increment(counts, key, truth(result.get(key)) ? 1 : 0);
                increment(counts, "failed_transactions", truth(result.get("successful_transactions")) ? 0 : 1);
                increment(counts, "completed");
                double completed = numeric(result.get("completed"));
                if (completed <= start + duration) {
                    increment(counts, "arrival_window_completed");
                    increment(counts, "arrival_window_sql_completed", truth(result.get("sql_completed")) ? 1 : 0);
                    increment(counts, "arrival_window_successful_transactions",
                            truth(result.get("successful_transactions")) ? 1 : 0);
                }
                Map<String, Object> values = object(perOp.get(op));
                increment(values, "completed");
                increment(values, "sql_completed", truth(result.get("sql_completed")) ? 1 : 0);
                increment(values, "successful_transactions", truth(result.get("successful_transactions")) ? 1 : 0);
                double latency = completed - scheduled;
                endToEnd.get(op).add(latency);
                int bucket = 0;
                while (bucket < LATENCY_BUCKETS.length && LATENCY_BUCKETS[bucket] < latency) bucket++;
                histogram[bucket]++;
                latencySum += latency;
                service.get(op).add(completed - numeric(result.get("started")));
                if (result.get("error") != null) increment(errors, result.get("error").toString());
            } finally {
                increment(counts, "in_flight", -1);
                lock.notifyAll();
            }
        }
    }

    Map<String, Object> finish(double finished, long atEnd) {
        List<Double> allEndToEnd = new ArrayList<>(), allService = new ArrayList<>(), oltp = new ArrayList<>();
        for (String op : OPERATIONS.keySet()) {
            allEndToEnd.addAll(endToEnd.get(op)); allService.addAll(service.get(op));
            if (!op.equals("report")) oltp.addAll(endToEnd.get(op));
            object(perOp.get(op)).put("latency_ms",
                    map("end_to_end", percentiles(endToEnd.get(op)), "service", percentiles(service.get(op))));
        }
        Map<String, Object> buckets = new LinkedHashMap<>();
        long total = 0;
        for (int i = 0; i < histogram.length; i++) buckets.put(BUCKET_NAMES[i], total += histogram[i]);
        double elapsed = finished - start;
        return map("phase", phase, "rps", rps, "arrival_duration_seconds", duration,
                "start_time", utcTimestamp(wallStart), "end_arrivals_time", utcTimestamp(wallStart + duration),
                "end_time", utcTimestamp(wallStart + elapsed), "start_epoch_seconds", wallStart,
                "end_arrivals_epoch_seconds", wallStart + duration, "end_epoch_seconds", wallStart + elapsed,
                "elapsed_seconds", elapsed, "drain_seconds", Math.max(0, finished - (start + duration)),
                "in_flight_at_arrival_end", atEnd, "counts", counts, "operations", perOp, "errors", errors,
                "throughput", map("offered_per_second", duration == 0 ? 0 : count(counts, "scheduled") / duration,
                        "successful_completions_per_second", count(counts, "successful_transactions") / elapsed,
                        "sql_completions_per_second", count(counts, "sql_completed") / elapsed,
                        "completion_elapsed_seconds", elapsed,
                        "arrival_window_successful_completions_per_second", duration == 0 ? 0
                                : count(counts, "arrival_window_successful_transactions") / duration,
                        "arrival_window_sql_completions_per_second", duration == 0 ? 0
                                : count(counts, "arrival_window_sql_completed") / duration),
                "error_fraction", count(counts, "dispatched") == 0 ? 0
                        : (double) count(counts, "failed_transactions") / count(counts, "dispatched"),
                "drop_fraction", count(counts, "scheduled") == 0 ? 0
                        : (double) count(counts, "dropped") / count(counts, "scheduled"),
                "telemetry", telemetry, "postgres_cpu_samples", cpuSamples, "postgres_cpu_sample_times", cpuTimes,
                "postgres_connection_samples", connectionSamples, "postgres_connection_sample_times", connectionTimes,
                "scheduled_latency_histogram", map("buckets", buckets, "count", counts.get("completed"),
                        "sum_seconds", latencySum),
                "latency_ms", map("end_to_end", percentiles(allEndToEnd), "service", percentiles(allService)),
                "oltp_latency_ms", percentiles(oltp), "olap_latency_ms", percentiles(endToEnd.get("report")));
    }
}

static Map<String, Object> runPhase(Args args, int rps, double duration, String phase,
                                   String level, long seed) throws Exception {
    return runPhase(args, rps, duration, phase, level, seed, args.mode.equals("both") ? "direct" : args.mode);
}

static Map<String, Object> runPhase(Args args, int rps, double duration, String phase,
                                   String level, long seed, String mode) throws Exception {
    return runPhase(args, rps, duration, phase, level, BigInteger.valueOf(seed), mode);
}

static Map<String, Object> runPhase(Args args, int rps, double duration, String phase,
                                   String level, BigInteger seed, String mode) throws Exception {
    Phase state = new Phase(args, rps, duration, phase, level, mode);
    long target = BigDecimal.valueOf(rps).multiply(BigDecimal.valueOf(duration))
            .setScale(0, RoundingMode.CEILING).longValueExact();
    Iterator<String> schedule = operations(seed, weights(args));
    WorkRandom seeds = new WorkRandom(seed);
    state.publish(false);
    state.wallStart = wallTime();
    state.start = monotonic();
    Thread telemetry = Thread.ofVirtual().name("load-telemetry").start(state::periodically);
    double finished;
    long atEnd;
    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
        for (long index = 0; index < target; index++) {
            double scheduled = state.start + (double) index / rps;
            sleep(scheduled - monotonic());
            String op = schedule.next();
            long workSeed = seeds.workSeed();
            synchronized (state.lock) {
                increment(state.counts, "scheduled");
                Map<String, Object> opCounts = object(state.perOp.get(op));
                increment(opCounts, "scheduled");
                if (count(state.counts, "in_flight") >= args.maxInFlight) {
                    increment(state.counts, "dropped"); increment(opCounts, "dropped");
                    continue;
                }
                increment(state.counts, "dispatched"); increment(state.counts, "in_flight");
                state.counts.put("peak_in_flight", Math.max(count(state.counts, "peak_in_flight"),
                        count(state.counts, "in_flight")));
                increment(opCounts, "dispatched");
                executor.execute(() -> {
                    Map<String, Object> result;
                    try { result = work(args, op, workSeed, phase, level); }
                    catch (Throwable failure) {
                        // Even an unexpected task failure must release the bounded drain.
                        result = map("accepted", false, "sql_completed", false,
                                "successful_transactions", false, "timeouts", false,
                                "error", failure.getClass().getSimpleName(), "started", monotonic(),
                                "completed", monotonic());
                    }
                    state.complete(op, scheduled, result);
                });
            }
        }
        sleep(state.start + duration - monotonic());
        synchronized (state.lock) {
            atEnd = count(state.counts, "in_flight");
            while (count(state.counts, "in_flight") != 0) state.lock.wait();
        }
        finished = monotonic();
    } finally {
        synchronized (state.stop) { state.stopped = true; state.stop.notifyAll(); }
        telemetry.join();
        state.publish(true);
    }
    return state.finish(finished, atEnd);
}

static Map<String, Object> snapshot(Args args, String path) {
    try {
        Response response = request(args.url, path, Math.min(args.controlTimeout, 10));
        return map("status", response.status, "response", response.data != null ? response.data : response.body);
    } catch (Exception e) { return map("error", e.toString()); }
}

static Map<String, Double> slowQueryMetrics(Args args) {
    try {
        Response response = request(args.ojpMetricsUrl, "/metrics", Math.min(args.controlTimeout, 10));
        if (response.status != 200) return null;
        Map<String, Double> totals = new LinkedHashMap<>();
        Pattern pattern = Pattern.compile("^ojp_sql_slow_executions_total(?:\\{([^}]*)\\})?\\s+([0-9.eE+-]+)$");
        Pattern labels = Pattern.compile("(\\w+)=\"([^\"]*)\"");
        for (String line : response.body.split("\\R")) {
            var match = pattern.matcher(line);
            if (!match.matches()) continue;
            String statement = "unlabeled";
            var label = labels.matcher(match.group(1) == null ? "" : match.group(1));
            while (label.find()) if (label.group(1).equals("sql_statement")) statement = label.group(2);
            totals.merge(statement, Double.parseDouble(match.group(2)), Double::sum);
        }
        return totals;
    } catch (Exception e) { return null; }
}

static Double metricValue(String text, String name) {
    Pattern pattern = Pattern.compile("^" + Pattern.quote(name) + "(?:\\{[^}]*\\})?\\s+([0-9.eE+-]+)$");
    for (String line : text.split("\\R")) {
        var match = pattern.matcher(line);
        if (match.matches()) return Double.parseDouble(match.group(1));
    }
    return null;
}

static Map<String, Object> runStep(Args args, String mode, int rps, int index) {
    String level = Integer.toString(rps);
    Map<String, Object> result = map("run_id", args.runId, "scenario", args.scenario, "mode", mode,
            "level", level, "warmup_only", args.warmupOnly, "rps", rps, "seed", args.seed,
            "request_timeout", args.requestTimeout, "control_timeout", args.controlTimeout,
            "max_in_flight", args.maxInFlight,
            "counter_semantics", map("accepted", "Application accepted=true if supplied, otherwise HTTP 2xx; admission is not SQL completion",
                    "failed_transactions", "Dispatched attempts without a successful transaction, including rejection, timeout, SQL failure and rollback",
                    "completed", "Finished HTTP attempts, including failures and timeouts"),
            "telemetry_counter_semantics", map("completed", "Successful transactions, as required by /arrivals",
                    "http_completed", "Finished HTTP attempts, matching phase counts.completed",
                    "sql_completed", "Synchronous SQL completions, including deliberate rollback"),
            "operation_weights", weights(args));
    Map<String, Double> before = mode.equals("ojp") ? slowQueryMetrics(args) : null;
    try {
        result.put("mode_response", control(args, "/mode", map("value", mode)));
        result.put("warmup_reset_response", control(args, "/reset"));
        Map<String, Object> context = map("phase", "warmup", "scenario", args.scenario,
                "level", level, "run_id", args.runId);
        control(args, "/context", context);
        result.put("readiness_before_warmup", readiness(args, level));
        control(args, "/reset");
        control(args, "/context", context);
        BigInteger stepSeed = args.seed.add(BigInteger.valueOf(index));
        Map<String, Object> warmup = runPhase(args, rps, args.warmup, "warmup", level, stepSeed, mode);
        result.put("warmup", warmup);
        if (args.warmupOnly) {
            if (args.warmup > 0 && count(object(warmup.get("counts")), "sql_completed") == 0)
                result.put("error", "warmup-only interval completed zero SQL operations");
            return result;
        }
        result.put("measurement_reset_response", control(args, "/reset"));
        result.put("readiness_before_measure", readiness(args, level));
        control(args, "/reset");
        context.put("phase", "measure");
        control(args, "/context", context);
        result.put("metrics_before_measure", snapshot(args, "/metrics"));
        Map<String, Object> measure = runPhase(args, rps, args.duration, "measure", level, stepSeed, mode);
        result.put("measure", measure);
        if (count(object(measure.get("counts")), "sql_completed") == 0)
            result.put("error", "measurement interval completed zero SQL operations");
        if (mode.equals("ojp")) {
            Map<String, Double> after = slowQueryMetrics(args);
            if (before != null && after != null) {
                Set<String> statements = new TreeSet<>(before.keySet());
                statements.addAll(after.keySet());
                Map<String, Object> deltas = new LinkedHashMap<>();
                double count = 0;
                for (String statement : statements) {
                    double delta = Math.max(0, after.getOrDefault(statement, 0.0) - before.getOrDefault(statement, 0.0));
                    deltas.put(statement, delta); count += delta;
                }
                result.put("slow_query_classification", map("observed", count > 0, "count", count,
                        "by_statement", deltas, "message", count > 0
                                ? "OJP classified slow SQL during this measurement."
                                : "No slow SQL classification was observed; segregation is enabled, but analytical-query isolation was not demonstrated."));
            } else {
                result.put("slow_query_classification", map("observed", null, "count", null,
                        "by_statement", map(), "message",
                        "OJP slow-query metrics were unavailable; classification could not be verified."));
            }
        }
    } catch (Exception e) {
        result.put("error", e.getMessage() == null ? e.toString() : e.getMessage());
    } finally {
        result.put("readiness_after", snapshot(args, "/ready"));
        result.put("metrics_after", snapshot(args, "/metrics"));
    }
    if (result.containsKey("measure")) publishBenchmark(args, mode, level, result);
    return result;
}

static void publishBenchmark(Args args, String mode, String level, Map<String, Object> result) {
    Map<String, Object> report = object(result.get("measure"));
    List<?> cpu = (List<?>) report.get("postgres_cpu_samples");
    List<?> physical = (List<?>) report.get("postgres_connection_samples");
    if (cpu.isEmpty() || physical.isEmpty()) {
        result.put("benchmark_result_error", "Measured result could not be published: no PostgreSQL CPU sample overlapped the measurement arrival window or physical connection samples were unavailable.");
        return;
    }
    Map<String, Object> oltp = object(report.get("oltp_latency_ms")), olap = object(report.get("olap_latency_ms"));
    Object slow = object(result.get("slow_query_classification")).get("count");
    Map<String, Object> values = map("mode", mode, "phase", "measure", "scenario", args.scenario, "level", level,
            "successful_rps", object(report.get("throughput")).get("successful_completions_per_second"),
            "oltp_p95_ms", oltp.get("p95") == null ? -1 : oltp.get("p95"),
            "olap_p95_ms", olap.get("p95") == null ? -1 : olap.get("p95"),
            "error_percent", numeric(report.get("error_fraction")) * 100,
            "dropped_requests", object(report.get("counts")).get("dropped"),
            "physical_connections", physical.stream().mapToDouble(value -> numeric(value)).average().orElseThrow(),
            "postgres_cpu_percent", cpu.stream().mapToDouble(value -> numeric(value)).average().orElseThrow(),
            "slow_classifications", slow == null ? 0 : slow);
    result.put("postgres_cpu_measurement_samples", cpu.size());
    result.put("postgres_cpu_sample_times", report.get("postgres_cpu_sample_times"));
    result.put("postgres_connection_measurement_samples", physical.size());
    result.put("postgres_connection_sample_times", report.get("postgres_connection_sample_times"));
    try { result.put("benchmark_result_published", control(args, "/benchmark-result", values)); }
    catch (Exception e) { result.put("benchmark_result_error", e.toString()); }
}

static String safeId(String id) { return id.replaceAll("[^a-zA-Z0-9_.-]", "_"); }
static String csvCell(Object value) {
    if (value == null) return "";
    String text = String.valueOf(value);
    if (text.contains(",") || text.contains("\"") || text.contains("\r") || text.contains("\n"))
        return "\"" + text.replace("\"", "\"\"") + "\"";
    return text;
}

static Path export(Args args, Map<String, Object> result, int step) throws IOException {
    Files.createDirectories(args.output);
    String stem = safeId(args.runId) + "-" + String.format(Locale.ROOT, "%03d", step) + "-"
            + result.get("mode") + "-" + result.get("level");
    Path json = args.output.resolve(stem + ".json"), csv = args.output.resolve(stem + ".csv");
    List<Map<String, Object>> rows = new ArrayList<>();
    for (String phase : List.of("warmup", "measure")) {
        if (!result.containsKey(phase)) continue;
        Map<String, Object> report = object(result.get(phase));
        List<String> ops = new ArrayList<>(List.of("all"));
        ops.addAll(OPERATIONS.keySet());
        for (String op : ops) {
            Map<String, Object> values = object(op.equals("all") ? report.get("counts")
                    : object(report.get("operations")).get(op));
            Map<String, Object> row = map("run_id", args.runId, "scenario", args.scenario,
                    "mode", result.get("mode"), "level", result.get("level"), "phase", phase, "operation", op,
                    "start_time", report.get("start_time"), "end_arrivals_time", report.get("end_arrivals_time"),
                    "end_time", report.get("end_time"), "errors_json", LoadJson.stringify(report.get("errors")),
                    "readiness_before_measure_json", LoadJson.stringify(result.get("readiness_before_measure")),
                    "readiness_after_json", LoadJson.stringify(result.get("readiness_after")));
            values.forEach((key, value) -> { if (!key.equals("latency_ms")) row.put(key, value); });
            if (op.equals("all")) {
                row.putAll(object(report.get("throughput")));
                row.put("error_fraction", report.get("error_fraction"));
                row.put("drop_fraction", report.get("drop_fraction"));
            }
            object(op.equals("all") ? report.get("latency_ms") : values.get("latency_ms"))
                    .forEach((kind, quantiles) -> object(quantiles)
                            .forEach((key, value) -> row.put(kind + "_" + key + "_ms", value)));
            rows.add(row);
        }
    }
    if (rows.isEmpty()) rows.add(map("run_id", args.runId, "mode", result.get("mode"),
            "level", result.get("level"), "error", result.getOrDefault("error", "")));
    Set<String> fields = new LinkedHashSet<>();
    rows.forEach(row -> fields.addAll(row.keySet()));
    StringBuilder csvText = new StringBuilder();
    csvText.append(String.join(",", fields)).append("\r\n");
    for (var row : rows) {
        boolean first = true;
        for (String field : fields) {
            if (!first) csvText.append(',');
            first = false;
            csvText.append(csvCell(row.get(field)));
        }
        csvText.append("\r\n");
    }
    // Stage in the destination filesystem. A hard link publishes complete bytes
    // atomically and fails if the final name exists (unlike ATOMIC_MOVE on Unix).
    Path jsonStage = args.output.resolve(stem + ".json." + UUID.randomUUID() + ".partial");
    Path csvStage = args.output.resolve(stem + ".csv." + UUID.randomUUID() + ".partial");
    boolean jsonPublished = false, csvPublished = false;
    try {
        Files.writeString(jsonStage, LoadJson.stringify(result) + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        Files.writeString(csvStage, csvText, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        Files.createLink(json, jsonStage);
        jsonPublished = true;
        Files.createLink(csv, csvStage);
        csvPublished = true;
    } finally {
        Files.deleteIfExists(jsonStage);
        Files.deleteIfExists(csvStage);
        if (jsonPublished && !csvPublished) Files.deleteIfExists(json);
    }
    return json;
}

static int run(String[] argv) {
    Args args;
    try { args = arguments(argv); }
    catch (HelpRequested e) { help(); return 0; }
    catch (IllegalArgumentException e) { System.err.println("ERROR: " + e.getMessage()); return 2; }
    if (args.validateOnly) return 0;
    if (args.scenario.equals("htap") && !args.mode.equals("direct")
            && !"true".equals(System.getenv("OJP_SLOW_QUERY_SEGREGATION_VERIFIED"))) {
        System.err.println("ERROR: OJP HTAP requires scripts/htap.sh to verify the running server's slow-query segregation configuration.");
        return 2;
    }
    try {
        if (Files.isDirectory(args.output)) {
            try (var files = Files.list(args.output)) {
                String prefix = safeId(args.runId) + "-";
                if (files.anyMatch(path -> Files.isRegularFile(path)
                        && path.getFileName().toString().startsWith(prefix)
                        && (path.toString().endsWith(".json") || path.toString().endsWith(".csv")))) {
                    System.err.println("ERROR: results already exist for run ID '" + args.runId + "' in "
                            + args.output + "; choose a unique --run-id");
                    return 2;
                }
            }
        }
        List<Map<String, Object>> failures = new ArrayList<>();
        int step = 0;
        for (String mode : args.mode.equals("both") ? List.of("direct", "ojp") : List.of(args.mode)) {
            for (int index = 0; index < args.rps.size(); index++) {
                int rps = args.rps.get(index);
                Map<String, Object> result = runStep(args, mode, rps, index);
                Path output = export(args, result, ++step);
                String phase = args.warmupOnly ? "warmup" : "measure";
                System.out.println(LoadJson.stringify(map("output", output.toString(), "mode", mode, "rps", rps,
                        "phase", phase, "counts", object(result.get(phase)).get("counts"),
                        "slow_query_classification", result.get("slow_query_classification"),
                        "benchmark_result_published", truth(result.get("benchmark_result_published")),
                        "error", result.get("error"))));
                if (result.containsKey("error")) failures.add(map("mode", mode, "rps", rps, "error", result.get("error")));
            }
        }
        if (!failures.isEmpty()) {
            System.err.println(LoadJson.stringify(map("benchmark_failures", failures)));
            return 1;
        }
        return 0;
    } catch (Exception e) { System.err.println("ERROR: " + e); return 1; }
}

void main(String[] args) { System.exit(run(args)); }
