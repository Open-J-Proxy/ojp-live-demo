package demo;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class Database implements AutoCloseable {
    private static final org.postgresql.Driver POSTGRES_DRIVER = new org.postgresql.Driver();
    private static final String DIRECT_APPLICATION_NAME = "demo-direct-" + UUID.randomUUID().toString().substring(0, 8);
    private final Config config;
    private final Metrics metrics;
    private final ReentrantReadWriteLock gate = new ReentrantReadWriteLock(true);
    private volatile HikariDataSource direct;
    volatile String mode = "starting", state = "starting", diagnostic = "Initializing database";
    volatile boolean ready;
    private long[] users, products;
    private volatile int directSessions = -1;
    private volatile String lastError = "";
    private volatile int postgresMaxConnections = -1, postgresSessions = -1, postgresActiveSessions = -1;
    private volatile int ojpSessions = -1;
    private volatile double postgresSnapshotTimestampSeconds;

    Database(Config config, Metrics metrics) { this.config = config; this.metrics = metrics; }

    void initialize() {
        try {
            Class.forName("org.postgresql.Driver");
            Class.forName("org.openjproxy.jdbc.Driver");
            long deadline = deadline();
            while (true) {
                try (Connection c = admin()) {
                    refreshPostgresDiagnostics(c);
                    users = ids(c, "SELECT id FROM users ORDER BY id");
                    products = ids(c, "SELECT id FROM products ORDER BY id");
                    try (PreparedStatement p = statement(c, "SELECT seed FROM benchmark_seed WHERE singleton");
                         ResultSet r = p.executeQuery()) {
                        if (!r.next()) throw new SQLException("Seed baseline missing");
                    }
                    if (users.length == 0 || products.length == 0) throw new SQLException("Seed users/products missing");
                    break;
                } catch (SQLException e) {
                    lastError = describe(e);
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
        String failure = describe(e);
        diagnostic += ": " + failure;
        if (lastError.isEmpty() || !(e instanceof TimeoutException)) lastError = failure;
        try (Connection c = admin()) { refreshPostgresDiagnostics(c); } catch (SQLException ignored) {}
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
        return POSTGRES_DRIVER.connect(Config.applicationName(env.getOrDefault("PG_ADMIN_URL", config.pgUrl()), "demo-admin"), p);
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
            long graceDeadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            boolean terminationAttempted = false;
            while (sessionCount() != 0) {
                if (!terminationAttempted && System.nanoTime() >= graceDeadline) {
                    diagnostic = "Terminating leftover sessions owned by " + DIRECT_APPLICATION_NAME;
                    terminateOwnedDirectSessions();
                    terminationAttempted = true;
                }
                if (System.nanoTime() >= deadline)
                    throw new TimeoutException("Direct PostgreSQL sessions remain after pool close: "
                            + directSessions + " owned by " + DIRECT_APPLICATION_NAME);
                Thread.sleep(100);
            }
            mode = next;
            metrics.mode = next;
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
        h.setDriverClassName("org.postgresql.Driver");
        h.setJdbcUrl(Config.applicationName(config.pgUrl(), DIRECT_APPLICATION_NAME));
        h.setUsername(config.pgUser()); h.setPassword(config.pgPassword());
        h.setPoolName("demo-direct");
        h.setMaximumPoolSize(config.poolSize()); h.setMinimumIdle(config.poolSize());
        h.setInitializationFailTimeout(-1); h.setConnectionTimeout(Math.min(30000L, config.startupSeconds() * 1000L));
        h.setMaxLifetime(0); h.setIdleTimeout(0);
        h.addDataSourceProperty("ApplicationName", DIRECT_APPLICATION_NAME);
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
                } catch (SQLException e) { lastError = describe(e); throw new CompletionException(e); }
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
            refreshPostgresDiagnostics(c);
            directSessions = Math.toIntExact(scalar(c,
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name='" + DIRECT_APPLICATION_NAME
                            + "' AND datname=current_database()"));
            return directSessions;
        }
    }

    private void terminateOwnedDirectSessions() throws SQLException {
        try (Connection c = admin();
             PreparedStatement p = c.prepareStatement("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                     + "WHERE application_name=? AND datname=current_database() AND usename=? AND pid<>pg_backend_pid()")) {
            p.setString(1, DIRECT_APPLICATION_NAME);
            p.setString(2, config.pgUser());
            p.execute();
        }
    }

    Map<String, Object> diagnostics(boolean probe) {
        boolean dbOk = false;
        if (probe) {
            try (Connection c = admin()) {
                refreshPostgresDiagnostics(c);
                dbOk = scalar(c, "SELECT 1") == 1;
                directSessions = Math.toIntExact(scalar(c,
                        "SELECT count(*) FROM pg_stat_activity WHERE application_name='" + DIRECT_APPLICATION_NAME
                                + "' AND datname=current_database()"));
            } catch (SQLException ignored) {}
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ready", ready && (!probe || dbOk)); result.put("mode", mode);
        result.put("state", state); result.put("diagnostic", diagnostic);
        result.put("database", dbOk); result.put("direct_sessions", directSessions);
        result.put("direct_pool_target", config.poolSize());
        result.put("ojp_pool_max", config.ojpPoolSize()); result.put("ojp_pool_min", config.ojpMinIdle());
        result.put("ojp_sessions", ojpSessions);
        result.put("logical_open", metrics.logicalOpen(mode.equals("ojp") ? "ojp" : "direct"));
        result.put("logical_open_by_mode", Map.of("direct", metrics.logicalOpen("direct"),
                "ojp", metrics.logicalOpen("ojp")));
        result.put("direct_application_name", DIRECT_APPLICATION_NAME);
        result.put("last_error", lastError);
        result.put("postgres_max_connections", postgresMaxConnections);
        result.put("postgres_sessions", postgresSessions);
        result.put("postgres_active_sessions", postgresActiveSessions);
        result.put("postgres_snapshot_timestamp_seconds", postgresSnapshotTimestampSeconds);
        return result;
    }

    private void refreshPostgresDiagnostics(Connection c) throws SQLException {
        try (PreparedStatement p = statement(c, "SELECT current_setting('max_connections')::int,"
                + "count(*) FILTER (WHERE backend_type='client backend'),"
                + "count(*) FILTER (WHERE backend_type='client backend' AND state='active') FROM pg_stat_activity");
             ResultSet r = p.executeQuery()) {
            r.next(); postgresMaxConnections = r.getInt(1); postgresSessions = r.getInt(2); postgresActiveSessions = r.getInt(3);
        }
        directSessions = Math.toIntExact(scalar(c, "SELECT count(*) FROM pg_stat_activity WHERE application_name='"
                + DIRECT_APPLICATION_NAME + "' AND datname=current_database()"));
        ojpSessions = Math.toIntExact(scalar(c,
                "SELECT count(*) FROM pg_stat_activity WHERE application_name='demo-ojp' AND datname=current_database()"));
        postgresSnapshotTimestampSeconds = System.currentTimeMillis() / 1000.0;
        metrics.physicalConnections(directSessions, ojpSessions);
    }

    private String describe(Throwable error) {
        StringBuilder text = new StringBuilder();
        for (int i = 0; error != null && i < 5; i++, error = error.getCause()) {
            if (!text.isEmpty()) text.append(" <- ");
            text.append(error.getClass().getSimpleName());
            if (error instanceof SQLException sql) text.append(" SQLState=").append(sql.getSQLState());
            if (error.getMessage() != null) text.append(": ").append(error.getMessage());
        }
        String value = text.toString();
        for (String secret : new String[]{config.pgPassword(), System.getenv().getOrDefault("PG_ADMIN_PASSWORD", "")})
            if (!secret.isEmpty()) value = value.replace(secret, "[redacted]");
        return value.substring(0, Math.min(2000, value.length()));
    }

    Map<String, Number> gauges() {
        Map<String, Number> result = new LinkedHashMap<>();
        result.put("ready", ready ? 1 : 0); result.put("direct_sessions", directSessions);
        result.put("postgres_sessions", postgresSessions);
        result.put("postgres_active_sessions", postgresActiveSessions);
        HikariDataSource pool = direct;
        var bean = pool == null ? null : pool.getHikariPoolMXBean();
        result.put("hikari_active", bean == null ? 0 : bean.getActiveConnections());
        result.put("hikari_idle", bean == null ? 0 : bean.getIdleConnections());
        result.put("hikari_total", bean == null ? 0 : bean.getTotalConnections());
        result.put("hikari_pending", bean == null ? 0 : bean.getThreadsAwaitingConnection());
        result.put("logical_open", metrics.logicalOpen(mode.equals("ojp") ? "ojp" : "direct"));
        result.put("direct_physical_connections", directSessions);
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
                metrics.connectionOpened(transactionMode);
            } finally { stats.acquisition.record(System.nanoTime() - acquireStart); }
            long rows = 0;
            try (AutoCloseable resource = () -> {
                try { c.close(); } finally { metrics.connectionClosed(transactionMode); }
            }) {
                long sqlStart = System.nanoTime();
                try {
                    c.setAutoCommit(false);
                    rows = execute(c, op, seed);
                    if (forceRollback) throw new ForcedRollback();
                    c.commit();
                } catch (Exception e) {
                    try { c.rollback(); stats.rollbacks.increment(); } catch (SQLException rollback) { e.addSuppressed(rollback); }
                    if (forceRollback && e instanceof ForcedRollback && e.getSuppressed().length == 0) stats.errors.increment();
                    else throw e;
                } finally { stats.sql.record(System.nanoTime() - sqlStart); }
            }
            if (!forceRollback) stats.completed.increment();
            Map<String, Object> result = new LinkedHashMap<>(Map.of("ok", true, "success", true,
                    "mode", transactionMode, "op", op, "rows", rows, "sql_completed", true,
                    "rollback", forceRollback, "completed", stats.completed.sum(),
                    "errors", stats.errors.sum(), "rollbacks", stats.rollbacks.sum()));
            result.put("rolledBack", forceRollback);
            return result;
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
            case "report" -> report(c, Math.floorMod(seed, 4));
            case "checkout", "rollback" -> checkout(c, user, random);
            case "review" -> update(c, "INSERT INTO reviews(user_id,product_id,rating,comment) VALUES(?,?,?,?)",
                    user, product, random.nextInt(1, 6), "Demo review");
            default -> throw new IllegalArgumentException("Unsupported operation");
        };
    }

    private long report(Connection c, int variant) throws SQLException {
        Timestamp from = Timestamp.valueOf("2024-01-01 00:00:00"), until = Timestamp.valueOf("2026-01-01 00:00:00");
        return switch (variant) {
            case 0 -> query(c, "SELECT date_trunc('day',o.order_date) AS day,count(DISTINCT o.id),sum(i.quantity),sum(i.quantity*p.price) AS revenue FROM orders o JOIN order_items i ON i.order_id=o.id JOIN products p ON p.id=i.product_id WHERE o.order_date>=? AND o.order_date<? GROUP BY day ORDER BY day", from, until);
            case 1 -> query(c, "SELECT p.id,p.name,sum(i.quantity) AS units,sum(i.quantity*p.price) AS revenue FROM order_items i JOIN products p ON p.id=i.product_id JOIN orders o ON o.id=i.order_id WHERE o.order_date>=? AND o.order_date<? GROUP BY p.id,p.name ORDER BY revenue DESC,p.id LIMIT 50", from, until);
            case 2 -> query(c, "SELECT u.id,u.username,count(DISTINCT o.id),sum(i.quantity),sum(i.quantity*p.price) AS spent FROM users u JOIN orders o ON o.user_id=u.id JOIN order_items i ON i.order_id=o.id JOIN products p ON p.id=i.product_id WHERE o.order_date>=? AND o.order_date<? GROUP BY u.id,u.username ORDER BY spent DESC,u.id LIMIT 50", from, until);
            default -> query(c, "SELECT p.id,p.name,count(r.id) AS reviews,avg(r.rating) AS rating FROM products p JOIN reviews r ON r.product_id=p.id WHERE r.id<=(SELECT reviews FROM benchmark_seed WHERE singleton) GROUP BY p.id,p.name ORDER BY reviews DESC,p.id LIMIT 50");
        };
    }

    private long checkout(Connection c, long user, SplittableRandom random) throws SQLException {
        int itemCount = random.nextInt(1, 6);
        long order;
        try (PreparedStatement p = statement(c,
                "INSERT INTO orders(user_id,order_date) VALUES(?,CURRENT_TIMESTAMP) RETURNING id")) {
            p.setLong(1, user);
            try (ResultSet r = p.executeQuery()) { r.next(); order = r.getLong(1); }
        }
        try (PreparedStatement product = statement(c, "SELECT price FROM products WHERE id=?");
             PreparedStatement item = statement(c, "INSERT INTO order_items(order_id,product_id,quantity) VALUES(?,?,?)")) {
            for (int i = 0; i < itemCount; i++) {
                long productId = products[random.nextInt(products.length)];
                int quantity = random.nextInt(1, 6);
                product.setLong(1, productId);
                try (ResultSet r = product.executeQuery()) {
                    if (!r.next()) throw new SQLException("Product missing");
                    r.getBigDecimal(1);
                }
                item.setLong(1, order); item.setLong(2, productId); item.setInt(3, quantity); item.executeUpdate();
            }
        }
        return itemCount;
    }

    synchronized void reset() throws Exception {
        if (!ready) throw new IllegalStateException("Service not ready");
        ready = false; state = "resetting"; diagnostic = "Draining transactions for baseline reset";
        if (!gate.writeLock().tryLock(config.startupSeconds(), TimeUnit.SECONDS)) {
            TimeoutException e = new TimeoutException("Reset drain timeout");
            fail(e); throw e;
        }
        try (Connection c = admin()) {
            c.setAutoCommit(false);
            try {
                query(c, "SELECT benchmark_reset()");
                c.commit();
            } catch (Exception e) { c.rollback(); throw e; }
            ready = true; state = "ready"; diagnostic = "Ready";
        } catch (Exception e) {
            fail(e); throw e;
        } finally { gate.writeLock().unlock(); }
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
    private static final class ForcedRollback extends SQLException {
        ForcedRollback() { super("Deliberate test-only failure after checkout inserts", "ZT001"); }
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
