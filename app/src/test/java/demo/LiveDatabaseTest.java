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
            long checkoutItemsForSeed = 0;
            for (String mode : new String[]{"direct", "ojp", "direct"}) {
                database.switchMode(mode);
                Map<String, Object> diagnostic = database.diagnostics(true);
                assertEquals(mode.equals("direct") ? config.poolSize() : 0, diagnostic.get("direct_sessions"), diagnostic.toString());
                assertTrue(((Number) diagnostic.get("postgres_max_connections")).intValue() > config.poolSize(), diagnostic.toString());
                assertTrue(((Number) diagnostic.get("postgres_sessions")).intValue() >= 1, diagnostic.toString());
                long insertedItems = 0;
                for (String op : new String[]{"browse", "history", "checkout", "review", "account", "report"}) {
                    var result = database.work(op, 1234, metrics.context, false);
                    assertEquals(true, result.get("ok"), mode + "/" + op);
                    assertEquals(true, result.get("success"), mode + "/" + op);
                    assertEquals(true, result.get("sql_completed"), mode + "/" + op);
                    assertEquals(op, result.get("op"));
                    if (op.equals("checkout")) insertedItems = ((Number) result.get("rows")).longValue();
                }
                assertTrue(insertedItems >= 1 && insertedItems <= 5);
                if (checkoutItemsForSeed == 0) checkoutItemsForSeed = insertedItems;
                assertEquals(checkoutItemsForSeed, insertedItems);
                for (int variant = 0; variant < 4; variant++)
                    assertEquals(true, database.work("report", variant, metrics.context, false).get("success"));
                assertEquals(orders + 1, count(database, "orders"));
                assertEquals(items + insertedItems, count(database, "order_items"));
                assertEquals(reviews + 1, count(database, "reviews"));
                var rollbackStats = metrics.stats(mode, "rollback", metrics.context);
                long previousErrors = rollbackStats.errors.sum(), previousRollbacks = rollbackStats.rollbacks.sum();
                assertEquals(true, database.work("rollback", 999, metrics.context, true).get("rollback"));
                assertEquals(previousErrors + 1, rollbackStats.errors.sum());
                assertEquals(previousRollbacks + 1, rollbackStats.rollbacks.sum());
                assertEquals(0, rollbackStats.completed.sum());
                assertEquals(orders + 1, count(database, "orders"));
                assertEquals(items + insertedItems, count(database, "order_items"));
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
