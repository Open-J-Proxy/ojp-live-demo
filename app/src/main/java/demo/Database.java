package demo;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class Database implements AutoCloseable {
    private final Config config;
    private final Metrics metrics;
    private final ReentrantReadWriteLock gate = new ReentrantReadWriteLock(true);
    private volatile HikariDataSource direct;
    volatile String mode = "starting", state = "starting", diagnostic = "Initializing database";
    volatile boolean ready;
    private long orderBaseline, reviewBaseline, itemBaseline;
    private long[] users, products;
    private volatile int directSessions = -1;

    Database(Config config, Metrics metrics) { this.config = config; this.metrics = metrics; }

    void initialize() {
        try {
            Class.forName("org.postgresql.Driver");
            Class.forName("org.openjproxy.jdbc.Driver");
            long deadline = deadline();
            while (true) {
                try (Connection c = admin()) {
                    users = ids(c, "SELECT id FROM users ORDER BY id");
                    products = ids(c, "SELECT id FROM products ORDER BY id");
                    try (PreparedStatement p = statement(c, "SELECT orders,items,reviews FROM benchmark_seed");
                         ResultSet r = p.executeQuery()) {
                        if (!r.next()) throw new SQLException("Seed baseline missing");
                        orderBaseline = r.getLong(1); itemBaseline = r.getLong(2); reviewBaseline = r.getLong(3);
                    }
                    if (users.length == 0 || products.length == 0) throw new SQLException("Seed users/products missing");
                    break;
                } catch (SQLException e) {
                    diagnostic = "Waiting for seeded PostgreSQL schema (" + e.getSQLState() + ")";
                    if (System.nanoTime() >= deadline) throw e;
                    Thread.sleep(500);
                }
            }
            switchMode(config.initialMode(), deadline);
        } catch (Exception e) { fail(e); }
    }

    private void fail(Exception e) {
        ready = false; state = "failed";
        diagnostic += ": " + (e instanceof SQLException sql
                ? "Database error SQLState=" + sql.getSQLState() : e.getClass().getSimpleName());
        System.err.println("Database initialization/switch failed: " + diagnostic);
    }

    Connection admin() throws SQLException {
        Map<String, String> env = System.getenv();
        Properties p = new Properties();
        p.setProperty("user", env.getOrDefault("PG_ADMIN_USER", config.pgUser()));
        p.setProperty("password", env.getOrDefault("PG_ADMIN_PASSWORD", config.pgPassword()));
        p.setProperty("ApplicationName", "demo-admin");
        p.setProperty("connectTimeout", "5");
        p.setProperty("socketTimeout", "10");
        return DriverManager.getConnection(env.getOrDefault("PG_ADMIN_URL", config.pgUrl()), p);
    }

    synchronized void switchMode(String next) throws Exception {
        if (state.equals("starting")) throw new IllegalStateException("Database initialization incomplete");
        switchMode(next, deadline());
    }

    private synchronized void switchMode(String next, long deadline) throws Exception {
        if (!Set.of("direct", "ojp").contains(next)) throw new IllegalArgumentException("value must be direct|ojp");
        if (users == null) throw new IllegalStateException("Database initialization incomplete");
        ready = false; state = "switching"; diagnostic = "Draining active transactions";
        if (!gate.writeLock().tryLock(config.startupSeconds(), TimeUnit.SECONDS)) {
            state = "failed"; diagnostic = "Timed out draining active transactions";
            throw new TimeoutException(diagnostic);
        }
        try {
            if (direct != null) { direct.close(); direct = null; }
            diagnostic = "Waiting for direct sessions to disappear";
            while (sessionCount() != 0) {
                checkDeadline(deadline);
                Thread.sleep(100);
            }
            mode = next;
            if (next.equals("direct")) {
                diagnostic = "Proactively filling direct pool";
                direct = pool();
                fill(deadline);
                while (sessionCount() < config.poolSize()) {
                    checkDeadline(deadline);
                    Thread.sleep(100);
                }
            } else {
                diagnostic = "Testing OJP transaction";
                probeOjp(deadline);
            }
            ready = true; state = "ready"; diagnostic = "Ready";
        } catch (Exception e) {
            if (direct != null) { direct.close(); direct = null; }
            fail(e);
            throw e;
        } finally { gate.writeLock().unlock(); }
    }

    private void probeOjp(long deadline) throws Exception {
        ExecutorService probe = Executors.newVirtualThreadPerTaskExecutor();
        Future<?> task = probe.submit(() -> {
            try (Connection c = DriverManager.getConnection(config.ojpUrl(), config.ojpProperties())) {
                c.setAutoCommit(false);
                try { scalar(c, "SELECT 1"); c.commit(); }
                catch (SQLException e) { c.rollback(); throw e; }
            } catch (SQLException e) { throw new CompletionException(e); }
        });
        try { task.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
        finally { task.cancel(true); probe.shutdownNow(); }
    }

    private HikariDataSource pool() {
        HikariConfig h = new HikariConfig();
        h.setJdbcUrl(config.pgUrl()); h.setUsername(config.pgUser()); h.setPassword(config.pgPassword());
        h.setPoolName("demo-direct");
        h.setMaximumPoolSize(config.poolSize()); h.setMinimumIdle(config.poolSize());
        h.setInitializationFailTimeout(-1); h.setConnectionTimeout(Math.min(30000L, config.startupSeconds() * 1000L));
        h.setMaxLifetime(0); h.setIdleTimeout(0);
        h.addDataSourceProperty("ApplicationName", "demo-direct");
        h.addDataSourceProperty("connectTimeout", "5");
        h.addDataSourceProperty("socketTimeout", Integer.toString(config.sqlTimeoutSeconds() + 10));
        return new HikariDataSource(h);
    }

    private void fill(long deadline) throws Exception {
        ExecutorService fill = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch release = new CountDownLatch(1), acquired = new CountDownLatch(config.poolSize());
        List<Future<?>> tasks = new ArrayList<>();
        HikariDataSource pool = direct;
        try {
            for (int i = 0; i < config.poolSize(); i++) tasks.add(fill.submit(() -> {
                try (Connection ignored = pool.getConnection()) {
                    acquired.countDown();
                    release.await();
                } catch (SQLException e) { throw new CompletionException(e); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }));
            if (!acquired.await(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                throw new TimeoutException("Direct pool did not reach " + config.poolSize() + " physical connections");
            release.countDown();
            for (Future<?> task : tasks) task.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } finally {
            release.countDown(); tasks.forEach(f -> f.cancel(true)); fill.shutdownNow();
        }
    }

    private int sessionCount() throws SQLException {
        try (Connection c = admin()) {
            directSessions = Math.toIntExact(scalar(c,
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name='demo-direct' AND datname=current_database()"));
            return directSessions;
        }
    }

    Map<String, Object> diagnostics(boolean probe) {
        boolean dbOk = false;
        if (probe) {
            try (Connection c = admin()) {
                dbOk = scalar(c, "SELECT 1") == 1;
                directSessions = Math.toIntExact(scalar(c,
                        "SELECT count(*) FROM pg_stat_activity WHERE application_name='demo-direct' AND datname=current_database()"));
            } catch (SQLException ignored) {}
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ready", ready && (!probe || dbOk)); result.put("mode", mode);
        result.put("state", state); result.put("diagnostic", diagnostic);
        result.put("database", dbOk); result.put("direct_sessions", directSessions);
        result.put("direct_pool_target", config.poolSize());
        result.put("ojp_pool_max", 40); result.put("ojp_pool_min", 10);
        result.put("logical_open", metrics.opened.sum() - metrics.closed.sum());
        return result;
    }

    Map<String, Number> gauges() {
        Map<String, Number> result = new LinkedHashMap<>();
        result.put("ready", ready ? 1 : 0); result.put("direct_sessions", directSessions);
        HikariDataSource pool = direct;
        var bean = pool == null ? null : pool.getHikariPoolMXBean();
        result.put("hikari_active", bean == null ? 0 : bean.getActiveConnections());
        result.put("hikari_idle", bean == null ? 0 : bean.getIdleConnections());
        result.put("hikari_total", bean == null ? 0 : bean.getTotalConnections());
        result.put("hikari_pending", bean == null ? 0 : bean.getThreadsAwaitingConnection());
        result.put("logical_open", metrics.opened.sum() - metrics.closed.sum());
        return result;
    }

    Map<String, Object> work(String op, long seed, Metrics.Context context, boolean forceRollback) throws Exception {
        if (!ready || !gate.readLock().tryLock()) throw new IllegalStateException("Service is switching/not ready");
        String transactionMode = mode;
        Metrics.Stats stats = metrics.stats(transactionMode, op, context);
        long start = System.nanoTime(), acquireStart = start;
        try {
            if (!ready) throw new IllegalStateException("Service is switching/not ready");
            Connection c;
            try {
                c = transactionMode.equals("direct") ? direct.getConnection()
                        : DriverManager.getConnection(config.ojpUrl(), config.ojpProperties());
                metrics.opened.increment();
            } finally { stats.acquisition.record(System.nanoTime() - acquireStart); }
            long rows;
            try (AutoCloseable resource = () -> { c.close(); metrics.closed.increment(); }) {
                long sqlStart = System.nanoTime();
                try {
                    c.setAutoCommit(false);
                    rows = execute(c, op, seed);
                    if (forceRollback) {
                        c.rollback(); stats.rollbacks.increment();
                    } else c.commit();
                } catch (Exception e) {
                    try { c.rollback(); stats.rollbacks.increment(); } catch (SQLException rollback) { e.addSuppressed(rollback); }
                    throw e;
                } finally { stats.sql.record(System.nanoTime() - sqlStart); }
            }
            stats.completed.increment();
            return Map.of("ok", true, "mode", transactionMode, "op", op, "rows", rows,
                    "rolledBack", forceRollback, "rollback", forceRollback, "completed", stats.completed.sum(),
                    "errors", stats.errors.sum(), "rollbacks", stats.rollbacks.sum());
        } catch (Exception e) {
            stats.errors.increment(); throw e;
        } finally {
            stats.request.record(System.nanoTime() - start);
            gate.readLock().unlock();
        }
    }

    private long execute(Connection c, String op, long seed) throws SQLException {
        SplittableRandom random = new SplittableRandom(seed);
        long user = users[random.nextInt(users.length)], product = products[random.nextInt(products.length)];
        return switch (op) {
            case "browse" -> query(c, "SELECT id,name,price FROM products WHERE id>=? ORDER BY id LIMIT 20", product);
            case "history" -> query(c, "SELECT o.id,o.order_date,i.product_id,i.quantity,p.price FROM orders o JOIN order_items i ON i.order_id=o.id JOIN products p ON p.id=i.product_id WHERE o.user_id=? ORDER BY o.id DESC LIMIT 50", user);
            case "account" -> query(c, "SELECT u.id,u.username,u.email,count(o.id) AS order_count FROM users u LEFT JOIN orders o ON o.user_id=u.id WHERE u.id=? GROUP BY u.id,u.username,u.email", user);
            case "report" -> query(c, "SELECT p.id,p.name,count(r.id),coalesce(avg(r.rating),0) FROM products p LEFT JOIN reviews r ON r.product_id=p.id WHERE p.id BETWEEN ? AND ? GROUP BY p.id,p.name ORDER BY count(r.id) DESC LIMIT 25", product, product + 100);
            case "checkout", "rollback" -> checkout(c, user, product);
            case "review" -> update(c, "INSERT INTO reviews(user_id,product_id,rating,comment) VALUES(?,?,?,?)",
                    user, product, random.nextInt(1, 6), "Demo review");
            default -> throw new IllegalArgumentException("Unsupported operation");
        };
    }

    private long checkout(Connection c, long user, long product) throws SQLException {
        try (PreparedStatement p = statement(c, "SELECT price FROM products WHERE id=?")) {
            p.setLong(1, product);
            try (ResultSet r = p.executeQuery()) {
                if (!r.next()) throw new SQLException("Product missing");
                r.getBigDecimal(1);
            }
        }
        long order;
        try (PreparedStatement p = statement(c,
                "INSERT INTO orders(user_id,order_date) VALUES(?,CURRENT_TIMESTAMP) RETURNING id")) {
            p.setLong(1, user);
            try (ResultSet r = p.executeQuery()) { r.next(); order = r.getLong(1); }
        }
        return update(c, "INSERT INTO order_items(order_id,product_id,quantity) VALUES(?,?,1)", order, product);
    }

    synchronized void reset() throws Exception {
        if (!ready) throw new IllegalStateException("Service not ready");
        if (!gate.writeLock().tryLock(config.startupSeconds(), TimeUnit.SECONDS)) throw new TimeoutException("Reset drain timeout");
        try (Connection c = admin()) {
            c.setAutoCommit(false);
            try {
                update(c, "DELETE FROM order_items WHERE order_id>?", orderBaseline);
                update(c, "DELETE FROM orders WHERE id>?", orderBaseline);
                update(c, "DELETE FROM reviews WHERE id>?", reviewBaseline);
                resetSequence(c, "orders", orderBaseline);
                resetSequence(c, "order_items", itemBaseline);
                resetSequence(c, "reviews", reviewBaseline);
                c.commit();
            } catch (Exception e) { c.rollback(); throw e; }
        } finally { gate.writeLock().unlock(); }
    }

    private void resetSequence(Connection c, String table, long baseline) throws SQLException {
        query(c, "SELECT setval(pg_get_serial_sequence(?, 'id'), ?, ?)", table, Math.max(1, baseline), baseline > 0);
    }

    private long[] ids(Connection c, String sql) throws SQLException {
        List<Long> ids = new ArrayList<>();
        try (PreparedStatement p = statement(c, sql); ResultSet r = p.executeQuery()) {
            while (r.next()) ids.add(r.getLong(1));
        }
        return ids.stream().mapToLong(Long::longValue).toArray();
    }
    private long scalar(Connection c, String sql) throws SQLException {
        try (PreparedStatement p = statement(c, sql); ResultSet r = p.executeQuery()) { r.next(); return r.getLong(1); }
    }
    private PreparedStatement statement(Connection c, String sql) throws SQLException {
        PreparedStatement p = c.prepareStatement(sql);
        try { p.setQueryTimeout(config.sqlTimeoutSeconds()); return p; }
        catch (SQLException e) { p.close(); throw e; }
    }
    private long query(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement p = statement(c, sql)) {
            bind(p, args);
            try (ResultSet r = p.executeQuery()) {
                long rows = 0;
                while (r.next()) {
                    rows++;
                    for (int i = 1; i <= r.getMetaData().getColumnCount(); i++) r.getObject(i);
                }
                return rows;
            }
        }
    }
    private long update(Connection c, String sql, Object... args) throws SQLException {
        try (PreparedStatement p = statement(c, sql)) { bind(p, args); return p.executeUpdate(); }
    }
    private static void bind(PreparedStatement p, Object[] args) throws SQLException {
        for (int i = 0; i < args.length; i++) p.setObject(i + 1, args[i]);
    }
    private long deadline() { return System.nanoTime() + Duration.ofSeconds(config.startupSeconds()).toNanos(); }
    private static void checkDeadline(long deadline) throws TimeoutException {
        if (System.nanoTime() >= deadline) throw new TimeoutException("Session/pool verification timed out");
    }
    @Override public void close() {
        ready = false;
        HikariDataSource pool = direct;
        if (pool != null) pool.close();
    }
}
