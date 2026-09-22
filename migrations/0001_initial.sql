CREATE TABLE IF NOT EXISTS skinloop_orders (
  komerza_order_id TEXT PRIMARY KEY,
  checkout_id TEXT UNIQUE,
  hosted_url TEXT,
  attempt INTEGER NOT NULL DEFAULT 1,
  idempotency_key TEXT NOT NULL UNIQUE,
  support_reference TEXT NOT NULL,
  customer_email TEXT NOT NULL,
  product_name TEXT NOT NULL DEFAULT '',
  original_amount REAL NOT NULL,
  original_currency TEXT NOT NULL,
  usd_amount_minor INTEGER NOT NULL,
  payment_status TEXT NOT NULL,
  fulfillment_allowed INTEGER NOT NULL DEFAULT 0,
  external_payment_id TEXT,
  delivery_state TEXT NOT NULL DEFAULT 'pending',
  delivery_lease_owner TEXT,
  delivery_lease_until TEXT,
  delivered_at TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  checkout_expires_at TEXT
);
CREATE TABLE IF NOT EXISTS skinloop_webhook_events (
  event_id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL,
  body_digest TEXT NOT NULL,
  komerza_order_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS skinloop_fulfillment_jobs (
  fulfillment_key TEXT PRIMARY KEY,
  checkout_id TEXT NOT NULL UNIQUE,
  komerza_order_id TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS skinloop_outbox (
  fulfillment_key TEXT PRIMARY KEY,
  order_id TEXT NOT NULL UNIQUE,
  published_at TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  publisher_owner TEXT,
  publisher_lease_until TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_delivery ON skinloop_orders(delivery_state, delivered_at);