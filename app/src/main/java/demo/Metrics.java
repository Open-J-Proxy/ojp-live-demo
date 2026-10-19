package demo;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

final class Metrics {
    static final Set<String> OPS = Set.of("browse", "history", "checkout", "review", "account", "report", "rollback", "hold");
    static final Set<String> SCENARIOS = Set.of("death-spiral", "slow-queries", "failover", "throttling", "integration");
    private final Set<Integer> levels = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<Labels, Stats> values = new ConcurrentHashMap<>();
    final LongAdder opened = new LongAdder(), closed = new LongAdder(), rejected = new LongAdder();
    volatile Context context = new Context("measure", "integration", 0);

    record Context(String phase, String scenario, int level) {}
    record Labels(String mode, String op, String phase, String scenario, int level) {
        String text() {
            return "mode=\"" + mode + "\",op=\"" + op + "\",phase=\"" + phase
                    + "\",scenario=\"" + scenario + "\",level=\"" + level + "\"";
        }
    }
    synchronized Context context(String phase, String scenario, int level) {
        if (!Set.of("warmup", "measure").contains(phase) || !SCENARIOS.contains(scenario)
                || level < 0 || level > 10000 || (!levels.contains(level) && levels.size() >= 64)) {
            throw new IllegalArgumentException("Invalid phase/scenario/level (maximum 64 distinct levels, range 0..10000)");
        }
        levels.add(level);
        return new Context(phase, scenario, level);
    }
    Stats stats(String mode, String op, Context c) {
        return values.computeIfAbsent(new Labels(mode, op, c.phase, c.scenario, c.level), ignored -> new Stats());
    }
    String render(Map<String, ? extends Number> gauges) {
        StringBuilder out = new StringBuilder();
        gauges.forEach((k, v) -> out.append("demo_").append(k).append(' ').append(v).append('\n'));
        out.append("demo_logical_connections_opened_total ").append(opened.sum()).append('\n')
                .append("demo_logical_connections_closed_total ").append(closed.sum()).append('\n')
                .append("demo_rejected_total ").append(rejected.sum()).append('\n');
        Context c = context;
        out.append("demo_context{phase=\"").append(c.phase).append("\",scenario=\"").append(c.scenario)
                .append("\",level=\"").append(c.level).append("\"} 1\n");
        values.forEach((labels, stats) -> {
            String l = labels.text();
            counter(out, "completions", l, stats.completed);
            counter(out, "errors", l, stats.errors);
            counter(out, "rollbacks", l, stats.rollbacks);
            stats.request.render(out, "request", l);
            stats.acquisition.render(out, "acquisition", l);
            stats.sql.render(out, "sql", l);
        });
        return out.toString();
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
