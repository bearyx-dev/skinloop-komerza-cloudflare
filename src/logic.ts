export const MAX_WEBHOOK_BYTES = 256_000;

export function cleanId(value: unknown): string {
  const s = String(value ?? "").trim();
  return /^[A-Za-z0-9_-]{8,200}$/.test(s) ? s : "";
}

export function usdMinor(value: unknown, currency: string, env: { USD_PER_EUR?: string; FX_BUFFER_BPS?: string }): number {
  const amount = Number(value);
  if (!Number.isFinite(amount) || amount <= 0) throw new Error("Invalid amount");
  const cents = BigInt(Math.round((amount + Number.EPSILON) * 100));
  if (currency.toUpperCase() === "USD") return Number(cents);
  if (currency.toUpperCase() !== "EUR") throw new Error("Unsupported currency");
  const rate = decimalScaled(env.USD_PER_EUR || "", 1_000_000);
  const buffer = Number(env.FX_BUFFER_BPS || "500");
  if (!Number.isInteger(buffer) || buffer < 0 || buffer > 5000) throw new Error("Invalid FX buffer");
  const n = cents * BigInt(rate) * BigInt(10_000 + buffer);
  const result = (n + 10_000_000_000n - 1n) / 10_000_000_000n;
  if (result <= 0n || result > 100_000_000n) throw new Error("Amount outside safe range");
  return Number(result);
}

function decimalScaled(value: string, scale: number): number {
  if (!/^\d+(?:\.\d{1,6})?$/.test(value)) throw new Error("Invalid EUR rate");
  const [whole, fraction = ""] = value.split(".");
  const result = BigInt(whole) * BigInt(scale) + BigInt((fraction + "000000").slice(0, 6));
  if (result <= 0n || result > BigInt(scale * 10)) throw new Error("Invalid EUR rate");
  return Number(result);
}

export function webhookAmountMinor(data: Record<string, unknown>, field = "amount"): number {
  const match = /^(?:0\.([0-9]{2})|([1-9][0-9]*)(?:\.([0-9]{2}))?)$/.exec(String(data[field] ?? ""));
  if (!match) return Number.NaN;
  const result = BigInt(match[2] || "0") * 100n + BigInt(match[1] || match[3] || "00");
  return result <= 100_000_000n ? Number(result) : Number.NaN;
}

export function webhookMatchesOrder(data: Record<string, unknown>, expected: { komerza_order_id: string; usd_amount_minor: number }): boolean {
  const amount = webhookAmountMinor(data);
  const hasRequired = data.requiredAmount !== undefined;
  const hasOverpayment = data.overpaymentAmount !== undefined;
  const amountOk = hasRequired || hasOverpayment
    ? hasRequired && hasOverpayment && webhookAmountMinor(data, "requiredAmount") === expected.usd_amount_minor &&
      amount >= expected.usd_amount_minor && webhookAmountMinor(data, "overpaymentAmount") === amount - expected.usd_amount_minor
    : amount === expected.usd_amount_minor;
  return data.merchantOrderId === expected.komerza_order_id && data.game === "rust" &&
    data.currency === "USD" && typeof data.externalPaymentId === "string" &&
    data.externalPaymentId.length > 0 && typeof data.reservationRequired === "boolean" && amountOk;
}

export function eventStateAllowed(type: string, state: string): boolean {
  return (type === "payment.pending" && state === "pending") ||
    (type === "payment.completed" && state === "completed") ||
    (type === "payment.reverted" && state === "reverted");
}

export function checkoutResponseValid(checkout: Record<string, any>, orderId: string, amountMinor: number, hostedOrigin: string): boolean {
  try {
    return Boolean(String(checkout.id || "").trim()) && String(checkout.merchantOrderId) === orderId &&
      Number(checkout.amount?.value ?? checkout.amount) === amountMinor &&
      String(checkout.amount?.currency || checkout.currency || "").toUpperCase() === "USD" &&
      Array.isArray(checkout.allowedGames) && checkout.allowedGames.length === 1 && checkout.allowedGames[0] === "rust" &&
      new URL(String(checkout.hostedUrl)).origin === new URL(hostedOrigin).origin;
  } catch { return false; }
}

export function deliveryResponseSuccessful(httpOk: boolean, body: Record<string, any>, komerzaStatus: string): boolean {
  return ["delivered", "fulfilled"].includes(komerzaStatus.toLowerCase()) || (httpOk && body.success === true);
}

export function rustCheckoutPayload(orderId: string, email: string, amountMinor: number, baseUrl: string, supportReference: string, expiresInSeconds: number) {
  return {
    merchantOrderId: orderId, customerEmail: email, amount: { value: amountMinor, currency: "USD" },
    allowedGames: ["rust"], successUrl: `${baseUrl}/return?order=${encodeURIComponent(orderId)}`,
    cancelUrl: `${baseUrl}/cancel?order=${encodeURIComponent(orderId)}`,
    metadata: { source: "komerza", game: "rust", supportReference },
    expiresInSeconds
  };
}

export async function verifySignature(rawBody: ArrayBuffer, eventId: string, timestamp: string, signature: string, secret: string): Promise<boolean> {
  const match = /^v1=([0-9a-f]{64})$/.exec(signature);
  if (!match || !secret || !/^[A-Za-z0-9_-]{8,200}$/.test(eventId)) return false;
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const prefix = new TextEncoder().encode(`${eventId}.${timestamp}.`);
  const message = new Uint8Array(prefix.length + rawBody.byteLength); message.set(prefix); message.set(new Uint8Array(rawBody), prefix.length);
  const actual = new Uint8Array(await crypto.subtle.sign("HMAC", key, message));
  const received = Uint8Array.from(match[1].match(/../g)!, x => parseInt(x, 16));
  return actual.length === received.length && actual.reduce((n, b, i) => n | (b ^ received[i]), 0) === 0;
}

export function validTimestamp(timestamp: string, now = Date.now()): boolean {
  if (!/^[0-9]{1,20}$/.test(timestamp)) return false;
  const seconds = Number(timestamp);
  return Number.isSafeInteger(seconds) && Math.abs(now - seconds * 1000) <= 300_000;
}