# Deployment runbook

1. Create a dedicated Cloudflare account/API authorization owned by the
   merchant. From this directory run `wrangler login`; the provisioning script
   uses that session and does not read or write credentials.
2. Run `bash scripts/provision.sh`; it creates queues/DLQ and D1, captures the
   D1 ID, updates config, and applies remote migrations. It does not deploy.
3. Set the safe variables for the merchant: Skinloop API base and hosted
   origins, worker `PUBLIC_BASE_URL`, `KOMERZA_STORE_ID`, `USD_PER_EUR`,
   `FX_BUFFER_BPS`, expiry, and `SHOP_URL`.
4. Add encrypted secrets interactively:

   ```sh
   wrangler secret put SKINLOOP_API_KEY
   wrangler secret put SKINLOOP_WEBHOOK_SECRET_CURRENT
   wrangler secret put KOMERZA_API_KEY
   # only while rotating:
   wrangler secret put SKINLOOP_WEBHOOK_SECRET_PREVIOUS
   ```

5. Run `bash scripts/deploy.sh`; it validates safe variables and verifies the
   three required encrypted secrets before deploying. Register
   `https://<worker-origin>/webhook/skinloop` for
   `payment.pending`, `payment.completed`, and `payment.reverted`.
7. Confirm the queue consumer has one-message batches, five-second timeout,
   ten retries, and the dedicated DLQ. Run the documented low-value smoke
   sequence before enabling the storefront button.

Never commit `wrangler.toml` values containing secrets, `.env` files, API
tokens, webhook signatures, or customer email addresses. Rotation procedure:
put the old secret in `SKINLOOP_WEBHOOK_SECRET_PREVIOUS`, replace current,
verify deliveries, then remove the previous secret.