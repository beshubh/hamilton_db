//! The `products` and `orders` tables, shaped like the ones in the CLI's
//! `hamilton.db` but scaled up, and the statements that create and fill
//! them. The rows come from a fixed seed, so every run, and both engines,
//! get the same data.

/// Product kinds: name prefix, category, brand and base price. Each
/// product is a kind with a number on the end, `laptop-0042`.
const KINDS: &[(&str, Option<&str>, Option<&str>, f64)] = &[
    ("camera", Some("photo"), Some("Canon"), 649.99),
    ("headphones", Some("audio"), Some("Sony"), 199.0),
    ("keyboard", Some("accessories"), Some("Logitech"), 89.5),
    ("laptop", Some("computers"), Some("Apple"), 1299.0),
    ("monitor", Some("computers"), Some("Dell"), 329.99),
    ("mouse", Some("accessories"), Some("Logitech"), 39.99),
    ("phone", Some("mobile"), Some("Apple"), 999.0),
    ("ps5", Some("gaming"), Some("Sony"), 499.0),
    ("router", Some("networking"), Some("TP-Link"), 129.0),
    ("speaker", Some("audio"), Some("JBL"), 149.5),
    ("ssd", Some("storage"), Some("Samsung"), 109.99),
    ("switch", Some("gaming"), Some("Nintendo"), 299.0),
    ("tablet", Some("mobile"), Some("Samsung"), 449.0),
    ("xbox", Some("gaming"), Some("Microsoft"), 499.0),
    ("webcam", Some("accessories"), Some("Logitech"), 79.0),
    ("drone", Some("photo"), Some("DJI"), 899.0),
    ("gift card", None, None, 25.0),
];

/// One order in this many is for an item that isn't in `products`, so
/// left joins have rows to pad.
const UNKNOWN_ITEM_EVERY: u64 = 50;

/// Rows per INSERT statement.
const ROWS_PER_INSERT: usize = 500;

pub const SCHEMA: &[&str] = &[
    "CREATE TABLE products (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL, \
     category VARCHAR(32), brand VARCHAR(32), price DOUBLE PRECISION, stock INT)",
    "CREATE INDEX idx_products_name ON products (name)",
    "CREATE TABLE orders (id BIGINT PRIMARY KEY, item VARCHAR(64), qty INT, \
     create_at INT, updated_at INT)",
    "CREATE INDEX idx_orders_item ON orders (item, qty)",
];

/// The INSERT statements for `products` products and `orders` orders.
pub fn inserts(products: usize, orders: usize) -> Vec<String> {
    let mut rng = Rng(0x5eed_1234_abcd_ef01);
    let names: Vec<String> = (0..products).map(product_name).collect();

    let product_rows = names.iter().enumerate().map(|(i, name)| {
        let (_, category, brand, base_price) = KINDS[i % KINDS.len()];
        // Within 50% of the base price, in whole cents.
        let cents = (base_price * 100.0 * (0.5 + rng.below(1000) as f64 / 1000.0)).round();
        let stock = match rng.below(20) {
            0 => "NULL".to_owned(),
            _ => rng.below(12).to_string(),
        };
        format!(
            "({}, {}, {}, {}, {}, {stock})",
            i + 1,
            quote(Some(name)),
            quote(category),
            quote(brand),
            cents / 100.0,
        )
    });
    let mut statements = batched("products", product_rows.collect());

    let order_rows = (0..orders as u64).map(|i| {
        let item = if rng.below(UNKNOWN_ITEM_EVERY) == 0 {
            format!("discontinued-{:04}", rng.below(100))
        } else {
            names[rng.below(products as u64) as usize].clone()
        };
        let create_at = 1_700_000_000 + i * 50 + rng.below(50);
        let updated_at = create_at + rng.below(86_400);
        format!(
            "({}, {}, {}, {create_at}, {updated_at})",
            i + 1,
            quote(Some(&item)),
            1 + rng.below(10),
        )
    });
    statements.extend(batched("orders", order_rows.collect()));
    statements
}

fn product_name(i: usize) -> String {
    format!("{}-{:04}", KINDS[i % KINDS.len()].0, i / KINDS.len())
}

fn batched(table: &str, rows: Vec<String>) -> Vec<String> {
    rows.chunks(ROWS_PER_INSERT)
        .map(|rows| format!("INSERT INTO {table} VALUES {}", rows.join(", ")))
        .collect()
}

fn quote(s: Option<&str>) -> String {
    match s {
        Some(s) => format!("'{}'", s.replace('\'', "''")),
        None => "NULL".to_owned(),
    }
}

/// xorshift64*: enough randomness for test data, and no dependency.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) % n
    }
}
