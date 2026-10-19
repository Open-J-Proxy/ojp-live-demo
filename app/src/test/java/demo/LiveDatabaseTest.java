package demo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "LIVE_DB_TESTS", matches = "true")
class LiveDatabaseTest {
    @Test void directAndOjpTransactionsRollbackResetAndSessionDisposal() throws Exception {
        Config config = Config.load(System.getenv());
        Metrics metrics = new Metrics();
        try (Database database = new Database(config, metrics)) {
            database.initialize();
            assertTrue(database.ready, database.diagnostics(true).toString());
            database.reset();
            long orders = count(database, "orders"), items = count(database, "order_items"), reviews = count(database, "reviews");
            for (String mode : new String[]{"direct", "ojp", "direct"}) {
                database.switchMode(mode);
                Map<String, Object> diagnostic = database.diagnostics(true);
                assertEquals(mode.equals("direct") ? config.poolSize() : 0, diagnostic.get("direct_sessions"), diagnostic.toString());
                for (String op : new String[]{"browse", "history", "checkout", "review", "account", "report"})
                    assertEquals(true, database.work(op, 1234, metrics.context, false).get("ok"), mode + "/" + op);
                assertEquals(orders + 1, count(database, "orders"));
                assertEquals(items + 1, count(database, "order_items"));
                assertEquals(reviews + 1, count(database, "reviews"));
                assertEquals(true, database.work("rollback", 999, metrics.context, true).get("rollback"));
                assertEquals(orders + 1, count(database, "orders"));
                assertEquals(items + 1, count(database, "order_items"));
                database.reset();
                assertEquals(orders, count(database, "orders"));
                assertEquals(items, count(database, "order_items"));
                assertEquals(reviews, count(database, "reviews"));
                assertEquals(metrics.opened.sum(), metrics.closed.sum());
            }
        }
    }
    private long count(Database database, String table) throws Exception {
        try (Connection c = database.admin();
             var p = c.prepareStatement("SELECT count(*) FROM " + table);
             var r = p.executeQuery()) {
            r.next(); return r.getLong(1);
        }
    }
}
