import test from "node:test";
import assert from "node:assert/strict";
import { checkoutResponseValid, deliveryResponseSuccessful, eventStateAllowed, rustCheckoutPayload, usdMinor, verifySignature, webhookMatchesOrder } from "../src/logic";

test("verifies the exact raw body signature", async () => {
    const body = new TextEncoder().encode('{"amount":"1.00"}').buffer;
    const id = "evt_test_123"; const ts = String(Math.floor(Date.now() / 1000));
    const key = await crypto.subtle.importKey("raw", new TextEncoder().encode("secret"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
    const prefix = new TextEncoder().encode(`${id}.${ts}.`);
    const message = new Uint8Array(prefix.length + body.byteLength); message.set(prefix); message.set(new Uint8Array(body), prefix.length);
    const mac = new Uint8Array(await crypto.subtle.sign("HMAC", key, message));
    const sig = `v1=${[...mac].map(x => x.toString(16).padStart(2, "0")).join("")}`;
    assert.equal(await verifySignature(body, id, ts, sig, "secret"), true);
    assert.equal(await verifySignature(new TextEncoder().encode("{}").buffer, id, ts, sig, "secret"), false);
});
test("accepts exact USD overpayment arithmetic", () => {
    const expected = { komerza_order_id: "order_123", usd_amount_minor: 126 };
    assert.equal(webhookMatchesOrder({ merchantOrderId: "order_123", game: "rust", currency: "USD", externalPaymentId: "p", reservationRequired: false, amount: "1.55", requiredAmount: "1.26", overpaymentAmount: "0.29" }, expected), true);
});
test("creates Rust-only checkout payload", () => {
    assert.deepEqual(rustCheckoutPayload("order_123", "a@example.com", 126, "https://worker.example", "RUST-1", 3600).allowedGames, ["rust"]);
});
test("converts EUR with a ceiling buffer", () => {
    assert.equal(usdMinor(1, "EUR", { USD_PER_EUR: "1.20", FX_BUFFER_BPS: "500" }), 126);
});