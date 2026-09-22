# Komerza → Skinloop Rust Worker

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/bearyx-dev/skinloop-komerza-cloudflare)

Standalone Cloudflare Worker template for merchants. It creates an immutable,
Rust-only Skinloop hosted checkout, validates signed webhooks over the exact raw
request bytes, accepts the documented USD overpayment arithmetic, and queues
delivery only after `completed` plus `fulfillmentAllowed: true`.

Cloudflare's deploy flow provisions the Worker, D1 database, fulfillment Queue,
dead-letter Queue, bindings, migrations, and scheduled outbox recovery in the
merchant's account. It prompts the merchant for required configuration; Komerza
and merchant Cloudflare credentials are never sent to or stored by Skinloop.

## Secrets (Cloudflare encrypted secrets only)

Required: `SKINLOOP_API_KEY`, `SKINLOOP_WEBHOOK_SECRET_CURRENT`,
`KOMERZA_API_KEY`. Optional during rotation:
`SKINLOOP_WEBHOOK_SECRET_PREVIOUS`. Never put these in source, D1, or `.env`.

Variables in `wrangler.toml` are safe configuration: API/origin URLs, worker
URL, Komerza store ID, EUR/USD rate, buffer, expiry, and shop URL. Replace all
example values before deployment.

## Provision and deploy

Install dependencies, authenticate as the merchant with `wrangler login`, then
provision resources and migrations:

```sh
npm install
bash scripts/provision.sh
# Edit wrangler.toml and supply every safe variable.
wrangler secret put SKINLOOP_API_KEY
wrangler secret put SKINLOOP_WEBHOOK_SECRET_CURRENT
wrangler secret put KOMERZA_API_KEY
bash scripts/deploy.sh
```

Configure the queue consumer with batch size 1, timeout 5 seconds, ten retries,
and `komerza-skinloop-rust-dead-letter` as its DLQ (the config declares these
bindings). Register `/webhook/skinloop` in Skinloop and allow Rust only.
The storefront sends customers to `/pay?ref=<unpaid Komerza order id>`.

Run `npm test` for pure invariant tests. A production smoke test should use a
low-value unpaid order, exact and overpaid Rust trades, pending/reverted events,
duplicate signed events, and a forced delivery retry; inspect D1 and the queue
dashboard without logging secrets or full payloads.