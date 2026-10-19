\set ON_ERROR_STOP on
SELECT EXISTS (SELECT 1 FROM benchmark_seed) AS seeded \gset
\if :seeded
SELECT seed = :seed AND users = :users AND products = :products
       AND orders = :orders AND items = :items AND reviews = :reviews AS matches
FROM benchmark_seed \gset
\if :matches
\echo 'Existing deterministic dataset retained.'
\else
\echo 'Dataset parameters changed. Use reset.sh --destroy-data explicitly before regeneration.'
\quit 1
\endif
\else
BEGIN;
SELECT setseed((((:seed::bigint % 2000000 + 2000000) % 2000000)::double precision / 1000000) - 1);
INSERT INTO users (id, username, email, created_at)
SELECT g, 'customer-' || g, 'customer-' || g || '@example.invalid',
       timestamp '2023-01-01' + (random() * 365 * interval '1 day')
FROM generate_series(1, :users) g;
INSERT INTO products (id, name, price)
SELECT g, (ARRAY['Camera','Laptop','Book','Headphones','Desk','Shoes','Coffee','Watch'])[1 + g % 8] || ' ' || g,
       round((5 + power(random(), 3) * 1995)::numeric, 2)
FROM generate_series(1, :products) g;
INSERT INTO orders (id, user_id, order_date)
SELECT g, 1 + floor(power(random(), 2) * :users)::bigint,
       timestamp '2024-01-01' + (random() * 730 * interval '1 day')
FROM generate_series(1, :orders) g;
INSERT INTO order_items (id, order_id, product_id, quantity)
SELECT g,
       1 + ((5 * ((g - 1) / 15) +
           CASE WHEN (g - 1) % 15 < 1 THEN 0
                WHEN (g - 1) % 15 < 3 THEN 1
                WHEN (g - 1) % 15 < 6 THEN 2
                WHEN (g - 1) % 15 < 10 THEN 3 ELSE 4 END) % :orders),
       1 + floor(power(random(), 2.5) * :products)::bigint,
       1 + floor(power(random(), 2) * 5)::int
FROM generate_series(1::bigint, :items::bigint) g;
INSERT INTO reviews (id, user_id, product_id, rating, comment)
SELECT g, o.user_id, i.product_id,
       1 + floor(power(random(), 0.5) * 5)::int,
       (ARRAY['Useful product, good value.','Delivery was quick.','Matches the description.',
              'Quality could be improved.','Would purchase again.'])[1 + g % 5]
FROM generate_series(1::bigint, :reviews::bigint) g
JOIN order_items i ON i.id = 1 + ((g - 1) % :items)
JOIN orders o ON o.id = i.order_id;
SELECT setval(pg_get_serial_sequence('users', 'id'), :users, true);
SELECT setval(pg_get_serial_sequence('products', 'id'), :products, true);
SELECT setval(pg_get_serial_sequence('orders', 'id'), :orders, true);
SELECT setval(pg_get_serial_sequence('order_items', 'id'), :items, true);
SELECT setval(pg_get_serial_sequence('reviews', 'id'), :reviews, true);
INSERT INTO benchmark_seed VALUES (true, :seed, :users, :products, :orders, :items, :reviews);
COMMIT;
\endif
SELECT (SELECT count(*) FROM users WHERE id <= :users) = :users
   AND (SELECT count(*) FROM products WHERE id <= :products) = :products
   AND (SELECT count(*) FROM orders WHERE id <= :orders) = :orders
   AND (SELECT count(*) FROM order_items WHERE id <= :items) = :items
   AND (SELECT count(*) FROM reviews WHERE id <= :reviews) = :reviews AS valid \gset
\if :valid
\echo 'All five baseline row counts verified; appended benchmark writes are retained until reset.'
\else
\echo 'Baseline row counts differ from the seed manifest. Explicit regeneration is required.'
\quit 1
\endif
ANALYZE;
