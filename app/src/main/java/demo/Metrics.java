package demo;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

final class Metrics {
    static final Set<String> OPS = Set.of("browse", "history", "checkout", "review", "account", "report", "rollback", "hold");
    static final Set<String> SCENARIOS = Set.of("death-spiral", "slow-queries", "failover", "throttling", "integration");
    private final Set<Double> levels = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<Labels, Stats> values = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<LoadLabels, Arrivals> arrivals = new ConcurrentHashMap<>();
    private volatile Map<String, Double> resources = Map.of();
    final LongAdder opened = new LongAdder(), closed = new LongAdder(), rejected = new LongAdder();
    volatile Context context = new Context("measure", "integration", 0);
    volatile String mode = "direct";
    private volatile double measurementStartSeconds;

    record Context(String phase, String scenario, double level) {}
    record Labels(String mode, String op, String phase, String scenario, double level) {
        String text() {
            return "mode=\"" + mode + "\",op=\"" + op + "\",phase=\"" + phase
                    + "\",scenario=\"" + scenario + "\",level=\"" + levelText(level) + "\"";
        }
    }
    record LoadLabels(String mode, String phase, String scenario, double level) {
        String text() {
            return "mode=\"" + mode + "\",phase=\"" + phase
                    + "\",scenario=\"" + scenario + "\",level=\"" + levelText(level) + "\"";
        }
    }
    record Latency(long count, double sumSeconds, List<Long> buckets) {
        static final String[] LIMITS = {"0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "30", "60", "+Inf"};
        Latency {
            buckets = List.copyOf(buckets);
            if (count < 0 || count > 1_000_000_000_000L || !Double.isFinite(sumSeconds) || sumSeconds < 0
                    || buckets.size() != LIMITS.length)
                throw new IllegalArgumentException("Invalid scheduled latency histogram");
            long previous = 0;
            for (long bucket : buckets) {
                if (bucket < previous || bucket > count) throw new IllegalArgumentException("Latency buckets must be cumulative");
                previous = bucket;
            }
            if (previous != count || (count == 0 && sumSeconds != 0))
                throw new IllegalArgumentException("Latency count must match +Inf bucket");
        }
        boolean follows(Latency before) {
            if (count < before.count || sumSeconds < before.sumSeconds) return false;
            for (int i = 0; i < buckets.size(); i++) if (buckets.get(i) < before.buckets.get(i)) return false;
            return true;
        }
        void render(StringBuilder out, String prefix, String labels) {
            for (int i = 0; i < buckets.size(); i++)
                sample(out, prefix + "_bucket", labels + ",le=\"" + LIMITS[i] + "\"", buckets.get(i));
            sample(out, prefix + "_count", labels, count);
            out.append(prefix).append("_sum{").append(labels).append("} ").append(sumSeconds).append('\n');
        }
    }
    record Arrivals(long scheduled, long dispatched, long dropped, long inflight, long completed, long errors,
                    long accepted, long sqlCompleted, long timeouts, Latency latency) {
        Arrivals(long scheduled, long dispatched, long dropped, long inflight, long completed, long errors) {
            this(scheduled, dispatched, dropped, inflight, completed, errors, 0, 0, 0, null);
        }
        Arrivals {
            for (long value : new long[]{scheduled, dispatched, dropped, inflight, completed, errors, accepted, sqlCompleted, timeouts})
                if (value < 0 || value > 1_000_000_000_000L) throw new IllegalArgumentException("Arrival counts outside 0..1000000000000");
            if (scheduled < dispatched + dropped || inflight + completed + errors > dispatched
                    || accepted > dispatched || sqlCompleted > completed + errors || timeouts > errors
                    || (latency != null && latency.count != completed + errors))
                throw new IllegalArgumentException("Inconsistent scheduled/dispatched/dropped/inflight/completed/errors");
        }
        boolean follows(Arrivals before) {
            return scheduled >= before.scheduled && dispatched >= before.dispatched && dropped >= before.dropped
                    && completed >= before.completed && errors >= before.errors
                    && accepted >= before.accepted && sqlCompleted >= before.sqlCompleted && timeouts >= before.timeouts
                    && (before.latency == null || (latency != null && latency.follows(before.latency)));
        }
    }
    void arrivals(String mode, Context context, Arrivals update, boolean reset) {
        if (!Set.of("direct", "ojp").contains(mode)) throw new IllegalArgumentException("mode must be direct|ojp");
        LoadLabels labels = new LoadLabels(mode, context.phase, context.scenario, context.level);
        arrivals.compute(labels, (ignored, previous) -> {
            if (!reset && previous != null && !update.follows(previous))
                throw new IllegalArgumentException("Cumulative arrivals must not decrease; use reset=true at phase start");
            return update;
        });
    }
    synchronized Context context(String phase, String scenario, double level) {
        if (level == 0) level = 0;
        if (!Set.of("warmup", "measure").contains(phase) || !SCENARIOS.contains(scenario)
                || !Double.isFinite(level) || level < 0 || level > 10000 || (!levels.contains(level) && levels.size() >= 64)) {
            throw new IllegalArgumentException("Invalid phase/scenario/level (maximum 64 distinct levels, range 0..10000)");
        }
        levels.add(level);
        return new Context(phase, scenario, level);
    }
    private static String levelText(double level) {
        return java.math.BigDecimal.valueOf(level).stripTrailingZeros().toPlainString();
    }
    synchronized void updateContext(Context next, boolean newRun) {
        if (next.phase.equals("warmup")) measurementStartSeconds = 0;
        else if (newRun || !next.equals(context) || measurementStartSeconds == 0)
            measurementStartSeconds = System.currentTimeMillis() / 1000.0;
        context = next;
    }
    Stats stats(String mode, String op, Context c) {
        return values.computeIfAbsent(new Labels(mode, op, c.phase, c.scenario, c.level), ignored -> new Stats());
    }
    void resources(double cpuPercent, double memoryPercent) {
        if (!Double.isFinite(cpuPercent) || cpuPercent < 0 || cpuPercent > 100000
                || !Double.isFinite(memoryPercent) || memoryPercent < 0 || memoryPercent > 100)
            throw new IllegalArgumentException("Invalid Docker CPU/memory percentages");
        resources = Map.of("postgres_cpu_percent", cpuPercent, "postgres_memory_percent", memoryPercent,
                "postgres_resource_timestamp_seconds", System.currentTimeMillis() / 1000.0);
    }
    String render(Map<String, ? extends Number> gauges) {
        StringBuilder out = new StringBuilder();
        gauges.forEach((k, v) -> out.append("demo_").append(k).append(' ').append(v).append('\n'));
        resources.forEach((k, v) -> out.append("demo_").append(k).append(' ').append(v).append('\n'));
        out.append("demo_logical_connections_opened_total ").append(opened.sum()).append('\n')
                .append("demo_logical_connections_closed_total ").append(closed.sum()).append('\n')
                .append("demo_rejected_total ").append(rejected.sum()).append('\n');
        Context c = context;
        out.append("demo_context{mode=\"").append(mode).append("\",phase=\"").append(c.phase).append("\",scenario=\"").append(c.scenario)
                .append("\",level=\"").append(levelText(c.level)).append("\"} 1\n");
        out.append("demo_measurement_start_timestamp_seconds ").append(measurementStartSeconds).append('\n');
        values.forEach((labels, stats) -> {
            String l = labels.text();
            counter(out, "completions", l, stats.completed);
            counter(out, "errors", l, stats.errors);
            counter(out, "rollbacks", l, stats.rollbacks);
            stats.request.render(out, "request", l);
            stats.acquisition.render(out, "acquisition", l);
            stats.sql.render(out, "sql", l);
        });
        arrivals.forEach((labels, load) -> {
            for (String prefix : new String[]{"benchmark_load_", "demo_load_"}) {
                String l = labels.text();
                sample(out, prefix + "scheduled_total", l, load.scheduled);
                sample(out, prefix + "dispatched_total", l, load.dispatched);
                sample(out, prefix + "dropped_total", l, load.dropped);
                sample(out, prefix + "completed_total", l, load.completed);
                sample(out, prefix + "errors_total", l, load.errors);
                sample(out, prefix + "inflight", l, load.inflight);
                sample(out, prefix + "accepted_total", l, load.accepted);
                sample(out, prefix + "sql_completed_total", l, load.sqlCompleted);
                sample(out, prefix + "timeouts_total", l, load.timeouts);
                if (load.latency != null) load.latency.render(out, prefix + "latency_seconds", l);
            }
        });
        return out.toString();
    }
    private static void sample(StringBuilder out, String name, String labels, long value) {
        out.append(name).append('{').append(labels).append("} ").append(value).append('\n');
    }
    private static void counter(StringBuilder out, String name, String labels, LongAdder value) {
        out.append("demo_").append(name).append("_total{").append(labels).append("} ").append(value.sum()).append('\n');
    }
    static final class Stats {
        final LongAdder completed = new LongAdder(), errors = new LongAdder(), rollbacks = new LongAdder();
        final Histogram request = new Histogram(), acquisition = new Histogram(), sql = new Histogram();
    }
    static final class Histogram {
        static final double[] LIMITS = {.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60};
        private final LongAdder[] buckets = new LongAdder[LIMITS.length];
        private final LongAdder count = new LongAdder(), nanos = new LongAdder();
        Histogram() { for (int i = 0; i < buckets.length; i++) buckets[i] = new LongAdder(); }
        void record(long elapsed) {
            count.increment(); nanos.add(elapsed);
            for (int i = 0; i < LIMITS.length; i++) if (elapsed / 1e9 <= LIMITS[i]) buckets[i].increment();
        }
        void render(StringBuilder out, String name, String labels) {
            String prefix = "demo_" + name + "_duration_seconds";
            for (int i = 0; i < LIMITS.length; i++)
                out.append(prefix).append("_bucket{").append(labels).append(",le=\"").append(LIMITS[i])
                        .append("\"} ").append(buckets[i].sum()).append('\n');
            out.append(prefix).append("_bucket{").append(labels).append(",le=\"+Inf\"} ").append(count.sum()).append('\n');
            out.append(prefix).append("_count{").append(labels).append("} ").append(count.sum()).append('\n');
            out.append(prefix).append("_sum{").append(labels).append("} ").append(nanos.sum() / 1e9).append('\n');
        }
    }
}
