package demo;

import java.util.Map;
import java.util.Properties;

record Config(String pgUrl, String pgUser, String pgPassword, String ojpUrl, String bind,
              int port, int adminPort, int poolSize, int workers, int queueSize,
              int startupSeconds, int sqlTimeoutSeconds, boolean testEndpoints, String initialMode) {
    static Config load(Map<String, String> env) {
        env = new java.util.HashMap<>(env);
        alias(env, "PG_URL", "DB_URL"); alias(env, "PG_USER", "DB_USER"); alias(env, "PG_PASSWORD", "DB_PASSWORD");
        alias(env, "HTTP_QUEUE_SIZE", "HTTP_QUEUE_CAPACITY"); alias(env, "STARTUP_TIMEOUT_SECONDS", "INIT_TIMEOUT_SECONDS");
        String pg = env.getOrDefault("PG_URL", "jdbc:postgresql://postgres:5432/shop");
        String ojp = env.getOrDefault("OJP_URL", "jdbc:ojp[ojp:1059]_postgresql://postgres:5432/shop");
        if (!pg.startsWith("jdbc:postgresql://") || !ojp.startsWith("jdbc:ojp[")) {
            throw new IllegalArgumentException("PG_URL must be PostgreSQL JDBC; OJP_URL must be OJP JDBC");
        }
        String mode = env.getOrDefault("MODE", "direct");
        if (!mode.equals("direct") && !mode.equals("ojp")) throw new IllegalArgumentException("MODE must be direct|ojp");
        int port = number(env, "PORT", 8080, 1, 65535);
        int admin = number(env, "ADMIN_PORT", 8081, 1, 65535);
        if (port == admin) throw new IllegalArgumentException("PORT and ADMIN_PORT must differ");
        return new Config(pg, env.getOrDefault("PG_USER", "demo"), env.getOrDefault("PG_PASSWORD", "demo"),
                ojp, env.getOrDefault("BIND_ADDRESS", "0.0.0.0"), port, admin,
                number(env, "DIRECT_POOL_SIZE", 200, 1, 2000),
                number(env, "HTTP_WORKERS", 512, 200, 2048),
                number(env, "HTTP_QUEUE_SIZE", 1024, 200, 10000),
                number(env, "STARTUP_TIMEOUT_SECONDS", 180, 1, 1800),
                number(env, "SQL_TIMEOUT_SECONDS", 30, 1, 600),
                Boolean.parseBoolean(env.getOrDefault("TEST_ENDPOINTS", "false")), mode);
    }

    private static void alias(Map<String, String> env, String preferred, String fallback) {
        if (!env.containsKey(preferred) && env.containsKey(fallback)) env.put(preferred, env.get(fallback));
    }

    private static int number(Map<String, String> env, String key, int fallback, int min, int max) {
        int value = Integer.parseInt(env.getOrDefault(key, Integer.toString(fallback)));
        if (value < min || value > max) throw new IllegalArgumentException(key + " outside " + min + ".." + max);
        return value;
    }

    Properties ojpProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", pgUser);
        properties.setProperty("password", pgPassword);
        properties.setProperty("ojp.connection.pool.maximumPoolSize", "40");
        properties.setProperty("ojp.connection.pool.minimumIdle", "10");
        properties.setProperty("ojp.connection.pool.connectionTimeout", "30000");
        properties.setProperty("ojp.jdbc.connection.close.synchronous", "true");
        return properties;
    }
}
