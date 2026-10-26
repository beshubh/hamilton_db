//! The join queries run against both engines.

pub struct Query {
    pub name: &'static str,
    pub sql: &'static str,
}

pub const QUERIES: &[Query] = &[
    // Hash join on a VARCHAR key, then a small aggregation and sort.
    Query {
        name: "units_by_category",
        sql: "SELECT p.category, count(*), sum(o.qty) \
              FROM orders o JOIN products p ON o.item = p.name \
              GROUP BY p.category ORDER BY p.category",
    },
    // Filters on both sides that predicate pushdown should move into the
    // scans, leaving a smaller join.
    Query {
        name: "big_orders_of_expensive_products",
        sql: "SELECT o.id, o.item, p.brand, o.qty \
              FROM orders o JOIN products p ON o.item = p.name \
              WHERE p.price > 500 AND o.qty >= 8",
    },
    // Every matching pair, so the cost of producing rows shows.
    Query {
        name: "all_matches",
        sql: "SELECT o.id, p.name, p.price, o.qty \
              FROM orders o JOIN products p ON o.item = p.name",
    },
    // A left join whose WHERE keeps only the padded rows: the orders for
    // items not in products.
    Query {
        name: "orders_of_unknown_items",
        sql: "SELECT o.item, count(*) \
              FROM orders o LEFT JOIN products p ON o.item = p.name \
              WHERE p.id IS NULL GROUP BY o.item",
    },
    // Both sides are the big table, and many orders share a key.
    Query {
        name: "self_join_same_item_and_qty",
        sql: "SELECT count(*) \
              FROM orders a JOIN orders b ON a.item = b.item AND a.qty = b.qty",
    },
    // No equality to hash on, so every pair is compared.
    Query {
        name: "qty_above_stock",
        sql: "SELECT count(*) \
              FROM orders o JOIN products p ON o.qty > p.stock \
              WHERE o.id <= 5000",
    },
];
