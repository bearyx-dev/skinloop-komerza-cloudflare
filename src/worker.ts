// @ts-nocheck
/**
 * Komerza Skinloop Rust-only payment bridge.
 *
 * Cloudflare bindings:
 *   DB                 D1 database
 *   FULFILLMENT_QUEUE  Cloudflare Queue
 *
 * Secrets:
 *   SKINLOOP_API_KEY
 *   SKINLOOP_WEBHOOK_SECRET_CURRENT
 *   SKINLOOP_WEBHOOK_SECRET_PREVIOUS (optional during rotation)
 *   KOMERZA_API_KEY
 *
 * Variables:
 *   SKINLOOP_API_BASE_URL
 *   SKINLOOP_HOSTED_ORIGIN
 *   PUBLIC_BASE_URL
 *   KOMERZA_STORE_ID
 *   USD_PER_EUR
 *   FX_BUFFER_BPS
 *   CHECKOUT_EXPIRES_SECONDS
 */

const KOMERZA_API = "https://api.komerza.com";
const ACTIVE_STATUSES = new Set([
  "creating",
  "created",
  "initiated",
  "pending",
  "active",
  "hold",
]);
const TERMINAL_STATUSES = new Set([
  "canceled",
  "cancelled",
  "declined",
  "failed",
  "expired",
]);
const RECONCILIATION_STATUSES = new Set([
  "reverted",
  "reconciliation_required",
]);
const WEBHOOK_EVENT_TYPES = new Set([
  "payment.pending",
  "payment.completed",
  "payment.reverted",
]);
const KOMERZA_UNAVAILABLE = new Set([
  "cancelled",
  "canceled",
  "completed",
  "complete",
  "paid",
  "delivered",
  "fulfilled",
]);
const KOMERZA_CANCELED = new Set(["cancelled", "canceled"]);
const KOMERZA_ALREADY_FULFILLED = new Set([
  "delivered",
  "fulfilled",
]);

const worker = {
  async fetch(request, env, ctx) {
    try {
      validateConfiguration(env);
      const url = new URL(request.url);

      if (request.method === "GET" && url.pathname === "/") {
        return json({
          ok: true,
          service: "Komerza Skinloop Rust Bridge",
        });
      }
      if (request.method === "GET" && url.pathname === "/pay") {
        return startCheckout(url, env);
      }
      if (request.method === "GET" && url.pathname === "/return") {
        return renderReturnPage(url, env);
      }
      if (request.method === "GET" && url.pathname === "/cancel") {
        return renderCancelPage(url, env);
      }
      if (request.method === "GET" && url.pathname === "/api/status") {
        return checkoutStatus(url, env);
      }
      if (request.method === "POST" && url.pathname === "/webhook/skinloop") {
        return receiveSkinloopWebhook(request, env);
      }

      return json({ error: "Not found" }, 404);
    } catch (error) {
      console.error("Unhandled bridge error", safeError(error));
      ctx.waitUntil(
        notifyDiscord(env, {
          title: "Skinloop bridge error",
          color: 0xdc2626,
          fields: [["Detail", safeError(error)]],
        }),
      );
      return json(
        {
          error: "The Rust-skins checkout is temporarily unavailable.",
        },
        500,
      );
    }
  },

  async queue(batch, env) {
    validateConfiguration(env);
    for (const message of batch.messages) {
      try {
        const orderId = cleanId(message.body?.orderId);
        const key = cleanText(message.body?.fulfillmentKey, 500);
        if (!orderId || !key) {
          message.ack();
          continue;
        }
        await fulfillOrder(orderId, key, env);
        message.ack();
      } catch (error) {
        console.error("Fulfillment queue failure", safeError(error));
        message.retry();
      }
    }
  },
  async scheduled(controller, env, ctx) {
    validateConfiguration(env);
    ctx.waitUntil(publishOutboxBatch(env, 25));
  },
};

async function startCheckout(url, env) {
  const orderId = cleanId(url.searchParams.get("ref"));
  if (!orderId) return errorPage("Missing Komerza order reference.", 400);

  const local = await getLocalOrder(orderId, env);
  if (
    local?.hosted_url &&
    ACTIVE_STATUSES.has(local.payment_status) &&
    Date.parse(local.checkout_expires_at || "") > Date.now()
  ) {
    return safeHostedRedirect(local.hosted_url, env);
  }
  if (local?.payment_status === "completed") {
    return Response.redirect(
      `${publicBase(env)}/return?order=${encodeURIComponent(orderId)}`,
      303,
    );
  }
  const recoverCreation =
    local &&
    !local.checkout_id &&
    ["creating", "create_failed"].includes(local.payment_status);
  if (local && !recoverCreation) {
    return errorPage(
      TERMINAL_STATUSES.has(local.payment_status)
        ? "This checkout has ended. Create a new Komerza order before trying again."
        : "This checkout has an unresolved payment state. Contact support before trying again.",
      409,
      local.support_reference,
    );
  }

  const order = await fetchEligibleKomerzaOrder(orderId, env);
  if (!order) {
    return errorPage(
      "This Komerza order cannot currently be paid.",
      404,
    );
  }

  const usdMinor = convertToUsdMinor(order.amount, order.currencyCode, env);
  if (
    recoverCreation &&
    (Math.round(Number(local.original_amount) * 100) !==
      Math.round(order.amount * 100) ||
      local.original_currency !== order.currencyCode ||
      Number(local.usd_amount_minor) !== usdMinor)
  ) {
    await markReconciliation(
      orderId,
      "Komerza order changed during checkout creation recovery",
      env,
    );
    return errorPage(
      "The Komerza order changed while checkout creation was being recovered.",
      409,
      local.support_reference,
    );
  }

  const attempt = recoverCreation ? Number(local.attempt) : 1;
  const supportReference = recoverCreation
    ? local.support_reference
    : createSupportReference();
  const idempotencyKey = recoverCreation
    ? local.idempotency_key
    : `skinloop:rust:v1:${orderId}:${attempt}`;
  const now = new Date().toISOString();

  if (recoverCreation) {
    await env.DB.prepare(
      `UPDATE skinloop_orders
          SET payment_status = 'creating', last_error = NULL, updated_at = ?
        WHERE komerza_order_id = ? AND checkout_id IS NULL
          AND idempotency_key = ?`,
    )
      .bind(now, orderId, idempotencyKey)
      .run();
  } else {
    await env.DB.prepare(
      `INSERT INTO skinloop_orders (
         komerza_order_id, attempt, idempotency_key, support_reference,
         customer_email, product_name, original_amount, original_currency,
         usd_amount_minor, payment_status, delivery_state, created_at, updated_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'creating', 'pending', ?, ?)`,
    )
      .bind(
        orderId,
        attempt,
        idempotencyKey,
        supportReference,
        order.customerEmail,
        order.productName,
        order.amount,
        order.currencyCode,
        usdMinor,
        now,
        now,
      )
      .run();
  }

  const response = await skinloopRequest(
    env,
    "/v1/merchant-api/checkouts",
    {
      method: "POST",
      idempotencyKey,
      body: {
        merchantOrderId: orderId,
          customerEmail: order.customerEmail,
        amount: { value: usdMinor, currency: "USD" },
        allowedGames: ["rust"],
        successUrl: `${publicBase(env)}/return?order=${encodeURIComponent(orderId)}`,
        cancelUrl: `${publicBase(env)}/cancel?order=${encodeURIComponent(orderId)}`,
        metadata: {
          supportReference,
          source: "komerza",
          game: "rust",
          originalAmount: String(order.amount),
          originalCurrency: order.currencyCode,
        },
        expiresInSeconds: checkoutExpiry(env),
      },
    },
  );

  if (!response.ok) {
    await env.DB.prepare(
      `UPDATE skinloop_orders
          SET payment_status = 'create_failed', last_error = ?, updated_at = ?
        WHERE komerza_order_id = ? AND attempt = ?`,
    )
      .bind(response.detail, new Date().toISOString(), orderId, attempt)
      .run();
    await notifyDiscord(env, {
      title: "Rust checkout creation failed",
      color: 0xef4444,
      fields: [
        ["Order", orderId],
        ["Support reference", supportReference],
        ["Detail", response.detail],
      ],
    });
    return errorPage(
      "The Rust-skins checkout could not be created.",
      502,
      supportReference,
    );
  }

  const checkout = unwrapCheckout(response.data);
  if (!validCreatedCheckout(checkout, orderId, usdMinor)) {
    await markReconciliation(
      orderId,
      "Skinloop create response did not match the stored order",
      env,
    );
    return errorPage(
      "Skinloop returned an unexpected checkout. No fulfilment will occur.",
      502,
      supportReference,
    );
  }

  const hostedUrl = validateHostedUrl(checkout.hostedUrl, env);
  if (!hostedUrl) {
    await markReconciliation(orderId, "Invalid Skinloop hosted URL", env);
    return errorPage(
      "Skinloop returned an invalid checkout URL.",
      502,
      supportReference,
    );
  }

  await env.DB.prepare(
    `UPDATE skinloop_orders
        SET checkout_id = ?, hosted_url = ?, checkout_expires_at = ?,
            payment_status = ?, updated_at = ?
      WHERE komerza_order_id = ? AND attempt = ? AND idempotency_key = ?`,
  )
    .bind(
      String(checkout.id),
      hostedUrl,
      String(checkout.expiresAt || ""),
      normalizeStatus(checkout.status || "created"),
      new Date().toISOString(),
      orderId,
      attempt,
      idempotencyKey,
    )
    .run();

  await notifyDiscord(env, {
    title: "Rust skins checkout started",
    color: 0x3b82f6,
    fields: [
      ["Product", order.productName || "Komerza order"],
      ["Order", orderId],
      ["Customer", order.customerEmail || "Not provided"],
      ["Skinloop amount", formatUsd(usdMinor)],
      ["Support reference", supportReference],
    ],
  });

  return Response.redirect(hostedUrl, 303);
}

async function checkoutStatus(url, env) {
  const orderId = cleanId(url.searchParams.get("order"));
  if (!orderId) return json({ error: "Missing order" }, 400);
  const local = await getLocalOrder(orderId, env);
  if (!local?.checkout_id) return json({ error: "Unknown checkout" }, 404);

  const refreshed = await refreshAuthoritativeStatus(local, env);
  if (refreshed.enqueue) {
    await publishOutbox(local, env);
  }
  return json({
    status: refreshed.status,
    delivered: refreshed.deliveryState === "delivered",
    fulfillmentAllowed: refreshed.fulfillmentAllowed,
    supportReference: local.support_reference,
    message: customerStatusMessage(
      refreshed.status,
      refreshed.deliveryState,
    ),
  });
}

async function receiveSkinloopWebhook(request, env) {
  const rawBody = await request.arrayBuffer();
  if (rawBody.byteLength > 256_000) {
    return rejectWebhook("payload_too_large", 413, {
      byteLength: rawBody.byteLength,
    });
  }

  const eventId = request.headers.get("Skinloop-Event-Id") || "";
  const timestamp = request.headers.get("Skinloop-Timestamp") || "";
  const signature = request.headers.get("Skinloop-Signature") || "";
  if (!validEventId(eventId) || !validTimestamp(timestamp)) {
    return rejectWebhook("invalid_signature_headers", 401, {
      eventId,
      hasTimestamp: Boolean(timestamp),
      hasSignature: Boolean(signature),
    });
  }
  if (
    !(await verifySkinloopSignatureWithRotation(
      rawBody,
      eventId,
      timestamp,
      signature,
      env,
    ))
  ) {
    return rejectWebhook("invalid_signature", 401, { eventId });
  }

  const rawText = new TextDecoder().decode(rawBody);
  let event;
  try {
    event = JSON.parse(rawText);
  } catch {
    return rejectWebhook("invalid_json", 400, {
      eventId,
    });
  }
  if (isSkinloopTestEvent(event, eventId)) {
    return json({ received: true, test: true });
  }
  if (
    !event ||
    event.version !== "1" ||
    event.id !== eventId ||
    !WEBHOOK_EVENT_TYPES.has(event.type) ||
    !event.data ||
    typeof event.data !== "object"
  ) {
    return rejectWebhook("invalid_event", 400, {
      headerEventId: eventId,
      bodyEventId: cleanText(event?.id, 200),
      version: event?.version,
      type: event?.type,
      hasData: Boolean(event?.data && typeof event.data === "object"),
    });
  }

  const digest = await sha256Hex(rawBody);
  const existing = await env.DB.prepare(
    `SELECT body_digest FROM skinloop_webhook_events WHERE event_id = ?`,
  )
    .bind(eventId)
    .first();
  if (existing) {
    if (existing.body_digest !== digest) {
      return rejectWebhook("event_identity_conflict", 409, { eventId });
    }
  }

  const data = event.data;
  if (!eventStatusAgrees(event.type, data.status)) {
    return rejectWebhook("event_status_mismatch", 400, { eventId });
  }
  const orderId = cleanId(data.merchantOrderId);
  if (!orderId) {
    return rejectWebhook("missing_order_identity", 400, {
      eventId,
    });
  }
  const local = await getLocalOrder(orderId, env);
  if (!local) {
    return rejectWebhook("unknown_order", 404, { eventId, orderId });
  }
  if (!webhookMatchesOrder(data, local)) {
    await markReconciliation(
      orderId,
      `Webhook identity conflict for event ${eventId}`,
      env,
    );
    return rejectWebhook("payment_identity_conflict", 409, {
      eventId,
      orderId,
      receivedGame: data.game,
      receivedCurrency: data.currency,
      receivedAmount: data.amount,
      receivedRequiredAmount: data.requiredAmount,
      receivedOverpaymentAmount: data.overpaymentAmount,
      expectedCurrency: "USD",
      expectedAmountMinor: Number(local.usd_amount_minor),
    });
  }

  const status = transitionPaymentStatus(local.payment_status, normalizeStatus(data.status));
  const fulfillmentAllowed =
    status === "completed" && data.fulfillmentAllowed === true;
  const receivedAt = new Date().toISOString();

  if (!existing) {
    const statements = [
      env.DB.prepare(
        `INSERT INTO skinloop_webhook_events
         (event_id, event_type, body_digest, komerza_order_id, created_at, received_at)
         VALUES (?, ?, ?, ?, ?, ?)`,
      ).bind(
        eventId,
        event.type,
        digest,
        orderId,
        String(event.createdAt || receivedAt),
        receivedAt,
      ),
       env.DB.prepare(
         `UPDATE skinloop_orders
             SET payment_status = CASE
                 WHEN payment_status IN ('completed','reverted','reconciliation_required','failed','declined','expired','canceled','cancelled')
                   AND ? IN ('pending','created','active','initiated') THEN payment_status
                 ELSE ? END,
                 fulfillment_allowed = CASE
                   WHEN ? IN ('reverted','reconciliation_required','failed','declined','expired','canceled','cancelled') THEN 0
                   WHEN ? = 'completed' THEN ? ELSE fulfillment_allowed END,
                external_payment_id = COALESCE(?, external_payment_id),
                updated_at = ?
           WHERE komerza_order_id = ?
             AND NOT (
               payment_status IN ('reverted','reconciliation_required','failed','declined','expired','canceled','cancelled')
               AND ? NOT IN ('reverted','reconciliation_required')
             )`,
      ).bind(
         status,
         status,
         status,
         status,
         fulfillmentAllowed ? 1 : 0,
        cleanText(data.externalPaymentId, 256),
        receivedAt,
        orderId,
         status,
      ),
    ];
    if (shouldEnqueueFulfillment(status, fulfillmentAllowed)) {
      statements.push(fulfillmentJobInsert(local, receivedAt, env));
      statements.push(
        env.DB.prepare(
          `INSERT OR IGNORE INTO skinloop_outbox
           (fulfillment_key, order_id, created_at, updated_at)
           VALUES (?, ?, ?, ?)`,
        ).bind(
          fulfillmentKey(orderId, local.checkout_id),
          orderId,
          receivedAt,
          receivedAt,
        ),
      );
    }
    await env.DB.batch(statements);
  }

  const persisted = await getLocalOrder(orderId, env);
  const canFulfillPersisted = persisted &&
    persisted.payment_status === "completed" &&
    Number(persisted.fulfillment_allowed) === 1;
  if (canFulfillPersisted) {
    await publishOutbox(persisted, env);
  } else if (!existing && RECONCILIATION_STATUSES.has(status)) {
    await notifyDiscord(env, {
      title: "Rust payment requires reconciliation",
      color: 0xf97316,
      fields: [
        ["Order", orderId],
        ["Status", status],
        ["Support reference", local.support_reference],
      ],
    });
  }

  return json({ received: true, duplicate: Boolean(existing) });
}

function rejectWebhook(error, status, details = {}) {
  console.warn("Skinloop webhook rejected", JSON.stringify({
    error, status, eventId: cleanText(details.eventId || details.headerEventId, 200),
  }));
  return json({ error }, status);
}

function isSkinloopTestEvent(event, headerEventId) {
  return (
    event?.type === "webhook.test" &&
    event.id === headerEventId &&
    typeof event?.data === "object" &&
    event.data !== null &&
    event.data.message === "Skinloop webhook test"
  );
}

async function refreshAuthoritativeStatus(local, env) {
  const response = await skinloopRequest(
    env,
    `/v1/merchant-api/checkouts/${encodeURIComponent(
      local.checkout_id,
    )}/status`,
  );
  if (!response.ok) {
    throw new Error(`Skinloop status request failed: ${response.detail}`);
  }
  const checkout = unwrapCheckout(response.data);
  if (!checkoutMatchesStoredOrder(checkout, local)) {
    await markReconciliation(
      local.komerza_order_id,
      "Skinloop status identity conflict",
      env,
    );
    throw new Error("Skinloop status identity conflict");
  }

  const status = transitionPaymentStatus(local.payment_status, normalizeStatus(checkout.status));
  const fulfillmentAllowed =
    status === "completed" && checkout.fulfillmentAllowed === true;
  const updatedAt = new Date().toISOString();
  const updateOrder = env.DB.prepare(
    `UPDATE skinloop_orders
        SET payment_status = ?, fulfillment_allowed = ?,
            external_payment_id = COALESCE(?, external_payment_id),
            updated_at = ?
       WHERE komerza_order_id = ?
         AND NOT (
           payment_status IN ('reverted','reconciliation_required','failed','declined','expired','canceled','cancelled')
           AND ? NOT IN ('reverted','reconciliation_required')
         )`,
  )
    .bind(
      status,
      fulfillmentAllowed ? 1 : 0,
      cleanText(checkout.externalPaymentId, 256),
      updatedAt,
      local.komerza_order_id,
      status,
    );
  if (shouldEnqueueFulfillment(status, fulfillmentAllowed)) {
    await env.DB.batch([
      updateOrder,
      fulfillmentJobInsert(local, updatedAt, env),
      env.DB.prepare(
        `INSERT OR IGNORE INTO skinloop_outbox
         (fulfillment_key, order_id, created_at, updated_at)
         VALUES (?, ?, ?, ?)`,
      ).bind(
        fulfillmentKey(local.komerza_order_id, local.checkout_id),
        local.komerza_order_id,
        updatedAt,
        updatedAt,
      ),
    ]);
  } else {
    await updateOrder.run();
  }

  const persisted = await getLocalOrder(local.komerza_order_id, env);
  const persistedStatus = normalizeStatus(persisted?.payment_status);
  const persistedAllowed = persistedStatus === "completed" &&
    Number(persisted?.fulfillment_allowed) === 1;
  return {
    status: persistedStatus,
    fulfillmentAllowed: persistedAllowed,
    deliveryState: persisted?.delivery_state,
    enqueue: shouldEnqueueFulfillment(persistedStatus, persistedAllowed),
  };
}

async function fulfillOrder(orderId, key, env) {
  const local = await getLocalOrder(orderId, env);
  if (!local?.checkout_id) throw new Error("Missing local checkout");
  await requireFulfillmentJob(local, key, env);
  if (local.delivered_at || local.delivery_state === "delivered") {
    await updateFulfillmentJob(key, "succeeded", "", env);
    return;
  }

  await env.DB.prepare(
    `UPDATE skinloop_fulfillment_jobs
        SET status = 'processing', attempt_count = attempt_count + 1,
            last_error = NULL, updated_at = ?
      WHERE fulfillment_key = ? AND komerza_order_id = ?
        AND status != 'succeeded'`,
  )
    .bind(new Date().toISOString(), key, orderId)
    .run();

  const authoritative = await refreshAuthoritativeStatus(local, env);
  if (
    authoritative.status !== "completed" ||
    !authoritative.fulfillmentAllowed
  ) {
    if (RECONCILIATION_STATUSES.has(authoritative.status)) {
      await updateFulfillmentJob(
        key,
        "needs_reconciliation",
        `Payment status: ${authoritative.status}`,
        env,
      );
    } else if (!terminalAuthoritativeStatus(authoritative.status)) {
      await updateFulfillmentJob(key, "queued", `Authoritative status: ${authoritative.status}`, env);
      throw new Error(`Transient authoritative payment status: ${authoritative.status}`);
    }
    return;
  }

  const leaseOwner = crypto.randomUUID();
  const leaseUntil = new Date(Date.now() + 300_000).toISOString();
  const acquired = await env.DB.prepare(
    `UPDATE skinloop_orders
        SET delivery_state = 'processing', delivery_lease_owner = ?,
            delivery_lease_until = ?, updated_at = ?
      WHERE komerza_order_id = ?
        AND delivered_at IS NULL
        AND payment_status = 'completed'
        AND fulfillment_allowed = 1
        AND (
          delivery_state != 'processing'
          OR delivery_lease_until IS NULL
          OR delivery_lease_until < ?
        )`,
  )
    .bind(
      leaseOwner,
      leaseUntil,
      new Date().toISOString(),
      orderId,
      new Date().toISOString(),
    )
    .run();
  if (!acquired.meta?.changes) return;

  const leased = await getLocalOrder(orderId, env);
  const confirmed = await refreshAuthoritativeStatus(leased, env);
  if (
    confirmed.status !== "completed" ||
    !confirmed.fulfillmentAllowed
  ) {
    await releaseDeliveryLease(orderId, leaseOwner, env);
    if (RECONCILIATION_STATUSES.has(confirmed.status)) {
      await updateFulfillmentJob(
        key,
        "needs_reconciliation",
        `Payment requires reconciliation: ${confirmed.status}`,
        env,
      );
      return;
    } else if (!terminalAuthoritativeStatus(confirmed.status)) {
      await updateFulfillmentJob(key, "queued", `Authoritative status: ${confirmed.status}`, env);
      throw new Error(`Transient authoritative payment status: ${confirmed.status}`);
    }
    return;
  }

  const komerza = await verifyKomerzaBeforeDelivery(leased, env);
  if (!komerza.ok) {
    await failDeliveryLease(orderId, leaseOwner, komerza.reason, env);
    await updateFulfillmentJob(
      key,
      "needs_reconciliation",
      komerza.reason,
      env,
    );
    await notifyDiscord(env, {
      title: "Rust payment confirmed — manual review required",
      color: 0xf97316,
      fields: [
        ["Order", orderId],
        ["Support reference", local.support_reference],
        ["Detail", komerza.reason],
      ],
    });
    return;
  }

  if (!(await renewDeliveryLease(orderId, leaseOwner, env))) {
    await releaseDeliveryLease(orderId, leaseOwner, env);
    await updateFulfillmentJob(
      key,
      "needs_reconciliation",
      "Payment state changed before delivery",
      env,
    );
    return;
  }

  const result = komerza.alreadyDelivered
    ? { ok: true, reason: "" }
    : await deliverKomerzaOrder(orderId, leased.checkout_id, env);
  if (!result.ok) {
    await failDeliveryLease(orderId, leaseOwner, result.reason, env);
    await updateFulfillmentJob(key, "queued", result.reason, env);
    await notifyDiscord(env, {
      title: "Rust payment confirmed — delivery failed",
      color: 0xef4444,
      fields: [
        ["Order", orderId],
        ["Support reference", local.support_reference],
        ["Detail", result.reason],
      ],
    });
    throw new Error(`Komerza delivery failed: ${result.reason}`);
  }

  const finalizedAt = new Date().toISOString();
  const results = await env.DB.batch([
    env.DB.prepare(
      `UPDATE skinloop_orders
        SET delivery_state = 'delivered', delivered_at = ?,
            delivery_lease_owner = NULL, delivery_lease_until = NULL,
            last_error = NULL, updated_at = ?
      WHERE komerza_order_id = ? AND delivery_lease_owner = ?
         AND delivery_state = 'processing'`,
    ).bind(finalizedAt, finalizedAt, orderId, leaseOwner),
    env.DB.prepare(
      `UPDATE skinloop_fulfillment_jobs
          SET status = 'succeeded', last_error = NULL, updated_at = ?
        WHERE fulfillment_key = ? AND komerza_order_id = ?`,
    ).bind(finalizedAt, key, orderId),
  ]);
  if (!results[0]?.meta?.changes) {
    throw new Error("Delivery completed but the D1 lease was no longer owned");
  }
  await notifyDiscord(env, {
    title: "Rust skins payment delivered",
    color: 0x22c55e,
    fields: [
      ["Product", local.product_name || "Komerza order"],
      ["Order", orderId],
      ["Customer", local.customer_email || "Not provided"],
      ["Skinloop amount", formatUsd(local.usd_amount_minor)],
      ["Support reference", local.support_reference],
    ],
  });
}

async function fetchEligibleKomerzaOrder(orderId, env) {
  const order = await fetchKomerzaOrder(orderId, env);
  if (!order) return null;

  const status = normalizeStatus(order.status);
  const amount = Number(order.amount || 0);
  const amountPaid = Number(order.amountPaid || 0);
  if (
    KOMERZA_UNAVAILABLE.has(status) ||
    !Number.isFinite(amount) ||
    amount <= 0 ||
    !Number.isFinite(amountPaid) ||
    amountPaid > 0
  ) {
    return null;
  }

  const customerEmail = customerEmailFromOrder(order);
  if (!customerEmail) return null;
  const items = Array.isArray(order.items) ? order.items : [];
  return {
    id: orderId,
    customerEmail,
    amount,
    currencyCode: String(order.currencyCode || "").toUpperCase(),
    productName: items
      .map((item) => cleanText(item?.productName, 200))
      .filter(Boolean)
      .join(", ")
      .slice(0, 500),
  };
}

async function fetchKomerzaOrder(orderId, env) {
  const response = await fetch(
    `${KOMERZA_API}/stores/${encodeURIComponent(
      env.KOMERZA_STORE_ID,
    )}/orders/${encodeURIComponent(orderId)}`,
    {
      headers: { Authorization: `Bearer ${env.KOMERZA_API_KEY}` },
      signal: AbortSignal.timeout(30_000),
    },
  );
  if (!response.ok) return null;
  const body = await response.json();
  const order = body?.data?.order;
  if (!order || String(order.id || "") !== orderId) return null;
  return order;
}

async function verifyKomerzaBeforeDelivery(local, env) {
  const order = await fetchKomerzaOrder(local.komerza_order_id, env);
  if (!order) return { ok: false, reason: "Komerza order was not found" };
  return classifyKomerzaForDelivery(order, local);
}

function classifyKomerzaForDelivery(order, local) {
  const status = normalizeStatus(order.status);
  const amount = Number(order.amount || 0);
  const amountPaid = Number(order.amountPaid || 0);
  if (KOMERZA_CANCELED.has(status)) {
    return { ok: false, reason: "Komerza order was canceled" };
  }
  if (
    !Number.isFinite(amount) ||
    Math.round(amount * 100) !==
      Math.round(Number(local.original_amount) * 100) ||
    String(order.currencyCode || "").toUpperCase() !== local.original_currency
  ) {
    return { ok: false, reason: "Komerza order amount or currency changed" };
  }
  if (KOMERZA_ALREADY_FULFILLED.has(status)) {
    return { ok: true, alreadyDelivered: true };
  }
  if (!Number.isFinite(amountPaid) || amountPaid !== 0) {
    return {
      ok: false,
      reason: "Komerza order has payment recorded but delivery is unconfirmed",
    };
  }
  return { ok: true, alreadyDelivered: false };
}

async function deliverKomerzaOrder(orderId, checkoutId, env) {
  const response = await fetch(
    `${KOMERZA_API}/stores/${encodeURIComponent(
      env.KOMERZA_STORE_ID,
    )}/orders/${encodeURIComponent(orderId)}/deliver`,
    {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${env.KOMERZA_API_KEY}`,
        "Idempotency-Key": `skinloop-delivery:${orderId}:${checkoutId}`,
      },
      signal: AbortSignal.timeout(30_000),
    },
  );
  const text = await response.text();
  try {
    const body = JSON.parse(text);
    return {
      ok: response.ok && body.success === true,
      reason:
        response.ok && body.success === true
          ? ""
          : cleanText(body.message, 500) || `HTTP ${response.status}`,
    };
  } catch {
    return {
      ok: false,
      reason: `Invalid Komerza response (HTTP ${response.status})`,
    };
  }
}

async function renewDeliveryLease(orderId, leaseOwner, env) {
  const renewed = await env.DB.prepare(
    `UPDATE skinloop_orders
        SET delivery_lease_until = ?, updated_at = ?
      WHERE komerza_order_id = ? AND delivery_lease_owner = ?
        AND delivery_state = 'processing' AND delivered_at IS NULL
        AND payment_status = 'completed'
        AND fulfillment_allowed = 1`,
  )
    .bind(
      new Date(Date.now() + 300_000).toISOString(),
      new Date().toISOString(),
      orderId,
      leaseOwner,
    )
    .run();
  return Boolean(renewed.meta?.changes);
}

async function releaseDeliveryLease(orderId, leaseOwner, env) {
  await env.DB.prepare(
    `UPDATE skinloop_orders
        SET delivery_state = 'pending', delivery_lease_owner = NULL,
            delivery_lease_until = NULL, updated_at = ?
      WHERE komerza_order_id = ? AND delivery_lease_owner = ?
        AND delivered_at IS NULL`,
  )
    .bind(new Date().toISOString(), orderId, leaseOwner)
    .run();
}

async function failDeliveryLease(orderId, leaseOwner, reason, env) {
  await env.DB.prepare(
    `UPDATE skinloop_orders
        SET delivery_state = 'failed', delivery_lease_owner = NULL,
            delivery_lease_until = NULL, last_error = ?, updated_at = ?
      WHERE komerza_order_id = ? AND delivery_lease_owner = ?
        AND delivered_at IS NULL`,
  )
    .bind(reason, new Date().toISOString(), orderId, leaseOwner)
    .run();
}

async function skinloopRequest(env, path, options = {}) {
  let response;
  try {
    response = await fetch(
      `${String(env.SKINLOOP_API_BASE_URL).replace(/\/+$/, "")}${path}`,
      {
        method: options.method || "GET",
        headers: {
          Authorization: `Bearer ${env.SKINLOOP_API_KEY}`,
          Accept: "application/json",
          ...(options.body ? { "Content-Type": "application/json" } : {}),
          ...(options.idempotencyKey
            ? { "Idempotency-Key": options.idempotencyKey }
            : {}),
        },
        body: options.body ? JSON.stringify(options.body) : undefined,
        signal: AbortSignal.timeout(30_000),
      },
    );
  } catch (error) {
    return {
      ok: false,
      status: 503,
      data: {},
      detail: `Skinloop network error: ${safeError(error)}`,
    };
  }
  const text = await response.text();
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = {};
  }
  if (!response.ok) {
    const requestId = response.headers.get("Skinloop-Request-Id") || "";
    return {
      ok: false,
      status: response.status,
      data,
      detail: [
        cleanText(data?.message || data?.error, 500) ||
          `Skinloop HTTP ${response.status}`,
        requestId ? `request ${requestId}` : "",
      ]
        .filter(Boolean)
        .join(" — "),
    };
  }
  return { ok: true, status: response.status, data };
}

function unwrapCheckout(data) {
  return data?.checkout || data?.data?.checkout || data?.data || data || {};
}

function validCreatedCheckout(checkout, orderId, usdMinor) {
  return (
    cleanText(checkout.id, 256) &&
    checkout.merchantOrderId === orderId &&
    checkoutAmount(checkout) === usdMinor &&
    checkoutCurrency(checkout) === "USD" &&
    Array.isArray(checkout.allowedGames) &&
    checkout.allowedGames.length === 1 &&
    checkout.allowedGames[0] === "rust"
  );
}

function checkoutMatchesStoredOrder(checkout, local) {
  return (
    String(checkout.id || "") === String(local.checkout_id) &&
    checkout.merchantOrderId === local.komerza_order_id &&
    checkoutAmount(checkout) === Number(local.usd_amount_minor) &&
    checkoutCurrency(checkout) === "USD"
  );
}

function webhookMatchesOrder(data, local) {
  const expectedAmount = Number(local.usd_amount_minor);
  const amount = webhookAmountMinor(data);
  const hasRequiredAmount = data.requiredAmount !== undefined;
  const hasOverpaymentAmount = data.overpaymentAmount !== undefined;
  const amountMatches = hasRequiredAmount || hasOverpaymentAmount
    ? hasRequiredAmount &&
      hasOverpaymentAmount &&
      webhookAmountMinor(data, "requiredAmount") === expectedAmount &&
      amount >= expectedAmount &&
      webhookAmountMinor(data, "overpaymentAmount") === amount - expectedAmount
    : amount === expectedAmount;

  return (
    data.merchantOrderId === local.komerza_order_id &&
    cleanText(data.externalPaymentId, 256).length > 0 &&
    data.game === "rust" &&
    typeof data.reservationRequired === "boolean" &&
    amountMatches &&
    data.currency === "USD"
  );
}

function checkoutAmount(checkout) {
  return Number(checkout?.amount?.value ?? checkout?.amount ?? NaN);
}

function webhookAmountMinor(data, field = "amount") {
  const value = String(data?.[field] ?? "");
  const match = /^(?:0\.([0-9]{2})|([1-9][0-9]*)(?:\.([0-9]{2}))?)$/.exec(
    value,
  );
  if (!match) return NaN;
  const whole = match[2] || "0";
  const fraction = match[1] || match[3] || "00";
  const minor = BigInt(whole) * 100n + BigInt(fraction);
  return minor <= 100_000_000n ? Number(minor) : NaN;
}

function checkoutCurrency(checkout) {
  return String(checkout?.amount?.currency || checkout?.currency || "").toUpperCase();
}

async function getLocalOrder(orderId, env) {
  return env.DB.prepare(
    `SELECT * FROM skinloop_orders WHERE komerza_order_id = ?`,
  )
    .bind(orderId)
    .first();
}

function fulfillmentKey(orderId, checkoutId) {
  return `skinloop:fulfillment:v1:${orderId}:${checkoutId}`;
}

function fulfillmentJobInsert(local, now, env) {
  return env.DB.prepare(
    `INSERT OR IGNORE INTO skinloop_fulfillment_jobs (
       fulfillment_key, checkout_id, komerza_order_id, status,
       attempt_count, created_at, updated_at
     ) VALUES (?, ?, ?, 'queued', 0, ?, ?)`,
  ).bind(
    fulfillmentKey(local.komerza_order_id, local.checkout_id),
    local.checkout_id,
    local.komerza_order_id,
    now,
    now,
  );
}

async function requireFulfillmentJob(local, key, env) {
  const job = await env.DB.prepare(
    `SELECT fulfillment_key, checkout_id, komerza_order_id, status
       FROM skinloop_fulfillment_jobs
      WHERE fulfillment_key = ? OR checkout_id = ? OR komerza_order_id = ?
      LIMIT 1`,
  )
    .bind(key, local.checkout_id, local.komerza_order_id)
    .first();
  const expected = fulfillmentKey(local.komerza_order_id, local.checkout_id);
  if (
    !job ||
    job.fulfillment_key !== expected ||
    job.fulfillment_key !== key ||
    job.checkout_id !== local.checkout_id ||
    job.komerza_order_id !== local.komerza_order_id
  ) {
    throw new Error("Fulfillment job identity conflict");
  }
  return job;
}

async function ensureFulfillmentJob(local, env) {
  if (!local?.checkout_id) throw new Error("Missing checkout for fulfillment job");
  const now = new Date().toISOString();
  await fulfillmentJobInsert(local, now, env).run();
  const key = fulfillmentKey(local.komerza_order_id, local.checkout_id);
  await requireFulfillmentJob(local, key, env);
  return key;
}

async function publishOutbox(local, env) {
  const key = await ensureFulfillmentJob(local, env);
  const current = new Date().toISOString();
  await env.DB.prepare(
    `INSERT OR IGNORE INTO skinloop_outbox
      (fulfillment_key, order_id, created_at, updated_at)
     VALUES (?, ?, ?, ?)`,
  ).bind(key, local.komerza_order_id, current, current).run();
  const result = await env.FULFILLMENT_QUEUE.send({
    orderId: local.komerza_order_id,
    fulfillmentKey: key,
  }).then(
    () => ({ published: true, error: "" }),
    (error) => ({ published: false, error: safeError(error) }),
  );
  await env.DB.prepare(
    `UPDATE skinloop_outbox
        SET attempts = attempts + 1, published_at = CASE WHEN ? = 1 THEN ? ELSE published_at END,
            last_error = ?, updated_at = ?
      WHERE fulfillment_key = ?`,
  ).bind(result.published ? 1 : 0, current, result.error, current, key).run();
  if (!result.published) throw new Error("Fulfillment outbox publish failed");
  return key;
}

async function publishOutboxBatch(env, limit = 25) {
  const rows = await env.DB.prepare(
    `SELECT o.fulfillment_key, o.order_id
       FROM skinloop_outbox o
      WHERE o.published_at IS NULL
        AND (o.publisher_lease_until IS NULL OR o.publisher_lease_until < ?)
      ORDER BY o.created_at ASC LIMIT ?`,
  ).bind(new Date().toISOString(), Math.min(25, Math.max(1, limit))).all();
  for (const row of rows.results || []) {
    const owner = crypto.randomUUID();
    const claimed = await env.DB.prepare(
      `UPDATE skinloop_outbox SET publisher_owner=?, publisher_lease_until=?,
          attempts=attempts+1, updated_at=?
       WHERE fulfillment_key=? AND published_at IS NULL
         AND (publisher_lease_until IS NULL OR publisher_lease_until < ?)`,
    ).bind(owner, new Date(Date.now() + 120_000).toISOString(), new Date().toISOString(),
      row.fulfillment_key, new Date().toISOString()).run();
    if (!claimed.meta?.changes) continue;
    try {
      const local = await getLocalOrder(row.order_id, env);
      if (!local || local.payment_status !== "completed" || !local.fulfillment_allowed) {
        await releaseOutboxPublisher(row.fulfillment_key, owner, "Payment is no longer deliverable", env);
        continue;
      }
      await env.FULFILLMENT_QUEUE.send({ orderId: row.order_id, fulfillmentKey: row.fulfillment_key });
      await env.DB.prepare(
        `UPDATE skinloop_outbox SET published_at=?, publisher_owner=NULL,
          publisher_lease_until=NULL, last_error=NULL, updated_at=?
         WHERE fulfillment_key=? AND publisher_owner=?`,
      ).bind(new Date().toISOString(), new Date().toISOString(), row.fulfillment_key, owner).run();
    } catch (error) {
      await releaseOutboxPublisher(row.fulfillment_key, owner, safeError(error), env);
    }
  }
}

function outboxPublishable(row, at = Date.now()) {
  return !row.published_at &&
    (!row.publisher_lease_until || Date.parse(row.publisher_lease_until) < at);
}

async function releaseOutboxPublisher(key, owner, error, env) {
  await env.DB.prepare(
    `UPDATE skinloop_outbox SET publisher_owner=NULL, publisher_lease_until=NULL,
      last_error=?, updated_at=? WHERE fulfillment_key=? AND publisher_owner=?`,
  ).bind(cleanText(error, 1000), new Date().toISOString(), key, owner).run();
}

async function updateFulfillmentJob(key, status, error, env) {
  await env.DB.prepare(
    `UPDATE skinloop_fulfillment_jobs
        SET status = ?, last_error = ?, updated_at = ?
      WHERE fulfillment_key = ?`,
  )
    .bind(status, cleanText(error, 1000), new Date().toISOString(), key)
    .run();
}

async function markReconciliation(orderId, reason, env) {
  await env.DB.prepare(
    `UPDATE skinloop_orders
        SET payment_status = 'reconciliation_required',
            last_error = ?, updated_at = ?
      WHERE komerza_order_id = ?`,
  )
    .bind(reason, new Date().toISOString(), orderId)
    .run();
  await notifyDiscord(env, {
    title: "Rust checkout requires reconciliation",
    color: 0xf97316,
    fields: [
      ["Order", orderId],
      ["Detail", reason],
    ],
  });
}

function convertToUsdMinor(amountMajor, currency, env) {
  if (!Number.isFinite(amountMajor) || amountMajor <= 0) {
    throw new Error("Invalid Komerza amount");
  }
  const sourceMinor = Math.round((amountMajor + Number.EPSILON) * 100);
  if (currency === "USD") return sourceMinor;
  if (currency !== "EUR") {
    throw new Error(`Unsupported Komerza currency: ${currency}`);
  }
  const rateScaled = decimalToScaled(env.USD_PER_EUR, 1_000_000);
  const bufferBps = integerInRange(env.FX_BUFFER_BPS ?? "500", 0, 5000);
  const numerator =
    BigInt(sourceMinor) *
    BigInt(rateScaled) *
    BigInt(10_000 + bufferBps);
  const denominator = 1_000_000n * 10_000n;
  const roundedUp = (numerator + denominator - 1n) / denominator;
  if (roundedUp <= 0n || roundedUp > 100_000_000n) {
    throw new Error("Converted Skinloop amount is outside the safe range");
  }
  return Number(roundedUp);
}

function decimalToScaled(value, scale) {
  const text = String(value || "").trim();
  if (!/^\d+(?:\.\d{1,6})?$/.test(text)) {
    throw new Error("USD_PER_EUR must be a positive decimal");
  }
  const [whole, fraction = ""] = text.split(".");
  const scaled =
    BigInt(whole) * BigInt(scale) +
    BigInt((fraction + "000000").slice(0, 6));
  if (scaled <= 0n || scaled > BigInt(scale * 10)) {
    throw new Error("USD_PER_EUR is outside the safe range");
  }
  return Number(scaled);
}

async function verifySkinloopSignature(
  rawBody,
  eventId,
  timestamp,
  signature,
  secret,
) {
  const match = /^v1=([0-9a-f]{64})$/.exec(signature);
  if (!match || !secret) return false;
  const prefix = new TextEncoder().encode(`${eventId}.${timestamp}.`);
  const message = new Uint8Array(prefix.length + rawBody.byteLength);
  message.set(prefix);
  message.set(new Uint8Array(rawBody), prefix.length);
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const expected = new Uint8Array(
    await crypto.subtle.sign("HMAC", key, message),
  );
  const received = hexToBytes(match[1]);
  return constantTimeEqual(expected, received);
}

async function verifySkinloopSignatureWithRotation(
  rawBody,
  eventId,
  timestamp,
  signature,
  env,
) {
  const [current, previous] = await Promise.all([
    verifySkinloopSignature(
      rawBody,
      eventId,
      timestamp,
      signature,
      env.SKINLOOP_WEBHOOK_SECRET_CURRENT,
    ),
    verifySkinloopSignature(
      rawBody,
      eventId,
      timestamp,
      signature,
      env.SKINLOOP_WEBHOOK_SECRET_PREVIOUS,
    ),
  ]);
  return current || previous;
}

function validTimestamp(value) {
  if (!/^[0-9]{1,20}$/.test(value)) return false;
  const seconds = Number(value);
  if (!Number.isSafeInteger(seconds)) return false;
  return Math.abs(Date.now() - seconds * 1000) <= 5 * 60 * 1000;
}

function validEventId(value) {
  return /^[A-Za-z0-9_-]{8,200}$/.test(value);
}

function constantTimeEqual(a, b) {
  if (a.length !== b.length) return false;
  let difference = 0;
  for (let index = 0; index < a.length; index += 1) {
    difference |= a[index] ^ b[index];
  }
  return difference === 0;
}

function hexToBytes(value) {
  const bytes = new Uint8Array(value.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

async function sha256Hex(value) {
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", value));
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function renderReturnPage(url, env) {
  const orderId = cleanId(url.searchParams.get("order"));
  if (!orderId) return errorPage("Missing order reference.", 400);
  return statusPage(
    "Confirming your Rust skins payment",
    "Keep this page open while we confirm the Steam trade and licence delivery.",
    orderId,
    env,
  );
}

function renderCancelPage(url, env) {
  const orderId = cleanId(url.searchParams.get("order"));
  return statusPage(
    "Rust skins checkout canceled",
    "No completed payment was confirmed. Return to the shop and create a new Komerza order before trying again.",
    orderId,
    env,
    false,
  );
}

function statusPage(title, message, orderId, env, poll = true) {
  const safeOrder = escapeHtml(orderId || "");
  const script = poll
    ? `<script>
      const output=document.getElementById("status");
      let checks=0;
      const timer=setInterval(async()=>{
        checks++;
        try{
          const response=await fetch("/api/status?order=${encodeURIComponent(
            orderId || "",
          )}",{cache:"no-store"});
          const data=await response.json();
          output.textContent=data.message||"Confirming payment…";
          if(data.delivered||["failed","declined","canceled","cancelled","expired","reverted","reconciliation_required"].includes(data.status)||checks>=120){
            clearInterval(timer);
          }
        }catch{output.textContent="Status temporarily unavailable. Do not create another payment yet."}
      },3000);
    </script>`
    : "";
  return new Response(
    `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escapeHtml(
      title,
    )}</title><style>
      *{box-sizing:border-box}html,body{min-height:100%;margin:0;background:#0b0b0d;color:#f5f5f7;font-family:Inter,system-ui,sans-serif}
      body{display:grid;place-items:center;padding:24px}.card{width:min(560px,100%);padding:32px;border:1px solid #27272a;border-radius:16px;background:#141416}
      h1{font-size:25px;margin:0 0 12px}p{color:#a1a1aa;line-height:1.6}.ref{margin-top:18px;padding:12px;border-radius:8px;background:#0b0b0d;font-family:monospace}
      a{display:inline-block;margin-top:20px;color:#6ff0dd}
    </style></head><body><main class="card"><h1>${escapeHtml(
      title,
    )}</h1><p>${escapeHtml(message)}</p><p id="status">${
      poll ? "Confirming payment…" : "Checkout canceled."
    }</p><div class="ref">Komerza order: ${safeOrder}</div><a href="${escapeHtml(
      env.SHOP_URL || "https://mintkeys.tech",
    )}">Return to shop</a></main>${script}</body></html>`,
    {
      status: 200,
      headers: {
        "Content-Type": "text/html;charset=UTF-8",
        "Cache-Control": "no-store",
        "Content-Security-Policy":
          "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'",
      },
    },
  );
}

function errorPage(message, status = 400, supportReference = "") {
  return new Response(
    `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Checkout unavailable</title><style>
      html,body{min-height:100%;margin:0;background:#0b0b0d;color:#f5f5f7;font-family:system-ui,sans-serif}body{display:grid;place-items:center;padding:24px}
      main{width:min(560px,100%);padding:32px;border:1px solid #3f2528;border-radius:16px;background:#141416}p{color:#fecaca;line-height:1.6}.ref{color:#a1a1aa;font-family:monospace}
    </style></head><body><main><h1>Checkout unavailable</h1><p>${escapeHtml(
      message,
    )}</p>${
      supportReference
        ? `<div class="ref">Support reference: ${escapeHtml(
            supportReference,
          )}</div>`
        : ""
    }</main></body></html>`,
    {
      status,
      headers: {
        "Content-Type": "text/html;charset=UTF-8",
        "Cache-Control": "no-store",
      },
    },
  );
}

function customerStatusMessage(status, deliveryState) {
  if (deliveryState === "delivered") return "Payment confirmed and order delivered.";
  if (status === "completed") return "Payment confirmed. Licence delivery is pending.";
  if (status === "hold") return "The Steam trade is on hold. Do not start another payment.";
  if (["pending", "active", "initiated", "created"].includes(status)) {
    return "The Steam trade is still being confirmed. Do not start another payment.";
  }
  if (status === "reverted" || status === "reconciliation_required") {
    return "This payment needs manual review. Contact support before trying again.";
  }
  if (TERMINAL_STATUSES.has(status)) {
    return `The checkout ended with status: ${status}. Create a new order to try again.`;
  }
  return "Confirming payment status.";
}

async function notifyDiscord() {
  // Intentionally disabled: this template never transmits customer/payment data
  // to a third-party notification service.
}

function validateConfiguration(env) {
  const required = [
    "SKINLOOP_API_BASE_URL",
    "SKINLOOP_API_KEY",
    "SKINLOOP_WEBHOOK_SECRET_CURRENT",
    "SKINLOOP_HOSTED_ORIGIN",
    "PUBLIC_BASE_URL",
    "KOMERZA_API_KEY",
    "KOMERZA_STORE_ID",
    "USD_PER_EUR",
  ];
  const missing = required.filter((name) => !String(env[name] || "").trim());
  if (missing.length) throw new Error(`Missing configuration: ${missing.join(", ")}`);
  if (!env.DB || !env.FULFILLMENT_QUEUE) {
    throw new Error("Missing DB or FULFILLMENT_QUEUE binding");
  }
  for (const name of [
    "SKINLOOP_API_BASE_URL",
    "SKINLOOP_HOSTED_ORIGIN",
    "PUBLIC_BASE_URL",
  ]) {
    const parsed = new URL(env[name]);
    if (parsed.protocol !== "https:") throw new Error(`${name} must use HTTPS`);
  }
}

function publicBase(env) {
  return String(env.PUBLIC_BASE_URL).replace(/\/+$/, "");
}

function checkoutExpiry(env) {
  return integerInRange(env.CHECKOUT_EXPIRES_SECONDS || "3600", 300, 86400);
}

function integerInRange(value, min, max) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    throw new Error(`Expected an integer from ${min} to ${max}`);
  }
  return parsed;
}

function validateHostedUrl(value, env) {
  try {
    const url = new URL(value);
    const allowed = new URL(env.SKINLOOP_HOSTED_ORIGIN);
    if (
      url.protocol !== "https:" ||
      url.origin !== allowed.origin ||
      url.username ||
      url.password
    ) {
      return "";
    }
    return url.toString();
  } catch {
    return "";
  }
}

function safeHostedRedirect(value, env) {
  const url = validateHostedUrl(value, env);
  return url
    ? Response.redirect(url, 303)
    : errorPage("Stored Skinloop URL is invalid.", 500);
}

function normalizeStatus(value) {
  return String(value || "").trim().toLowerCase();
}

function eventStatusAgrees(type, value) {
  const state = normalizeStatus(value);
  return (
    (type === "payment.pending" && state === "pending") ||
    (type === "payment.completed" && state === "completed") ||
    (type === "payment.reverted" && state === "reverted")
  );
}

const PAYMENT_TRANSITIONS = Object.freeze({
  creating: new Set(["creating", "created", "pending", "active", "initiated", "hold", "completed", "reverted", "reconciliation_required", "failed", "declined", "expired", "canceled", "cancelled"]),
  create_failed: new Set(["creating", "created", "pending", "active", "initiated", "hold", "completed", "reverted", "reconciliation_required", "failed", "declined", "expired", "canceled", "cancelled"]),
  created: new Set(["created", "pending", "active", "initiated", "hold", "completed", "reverted", "reconciliation_required", "failed", "declined", "expired", "canceled", "cancelled"]),
  pending: new Set(["pending", "active", "initiated", "hold", "completed", "reverted", "reconciliation_required", "failed", "declined", "expired", "canceled", "cancelled"]),
  active: new Set(["active", "pending", "initiated", "hold", "completed", "reverted", "reconciliation_required", "failed", "declined", "expired", "canceled", "cancelled"]),
  initiated: new Set(["initiated", "pending", "active", "hold", "completed", "reverted", "reconciliation_required", "failed", "declined", "expired", "canceled", "cancelled"]),
  hold: new Set(["hold", "completed", "reverted", "reconciliation_required", "failed", "declined", "expired", "canceled", "cancelled"]),
  completed: new Set(["completed", "reverted", "reconciliation_required", "failed", "declined", "expired", "canceled", "cancelled"]),
  failed: new Set(["failed", "reconciliation_required"]),
  declined: new Set(["declined", "reconciliation_required"]),
  expired: new Set(["expired", "reconciliation_required"]),
  canceled: new Set(["canceled", "reconciliation_required"]),
  cancelled: new Set(["cancelled", "reconciliation_required"]),
  reverted: new Set(["reverted", "reconciliation_required"]),
  reconciliation_required: new Set(["reconciliation_required"]),
});

function transitionPaymentStatus(current, next) {
  const from = normalizeStatus(current) || "creating";
  const to = normalizeStatus(next);
  return PAYMENT_TRANSITIONS[from]?.has(to) ? to : from;
}

function terminalAuthoritativeStatus(value) {
  return RECONCILIATION_STATUSES.has(normalizeStatus(value)) ||
    TERMINAL_STATUSES.has(normalizeStatus(value));
}

function shouldRetryAuthoritativeStatus(value) {
  return !terminalAuthoritativeStatus(value);
}

function shouldEnqueueFulfillment(status, fulfillmentAllowed) {
  return normalizeStatus(status) === "completed" && fulfillmentAllowed === true;
}

function cleanId(value) {
  const text = String(value || "").trim();
  return /^[A-Za-z0-9_-]{8,200}$/.test(text) ? text : "";
}

function cleanText(value, max = 500) {
  return String(value || "").trim().slice(0, max);
}

function normalizeCustomerEmail(value) {
  const email = String(value || "").trim();
  if (
    email.length < 3 ||
    email.length > 320 ||
    !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
  ) {
    return "";
  }
  return email;
}

function customerEmailFromOrder(order) {
  return normalizeCustomerEmail(
    order?.customer?.emailAddress ?? order?.customerEmail,
  );
}

function createSupportReference() {
  return `MK-RUST-${crypto.randomUUID().replace(/-/g, "").slice(0, 8).toUpperCase()}`;
}

function formatUsd(minor) {
  return `$${(Number(minor) / 100).toFixed(2)} USD`;
}

function safeError(error) {
  return error instanceof Error
    ? error.message.slice(0, 1000)
    : String(error).slice(0, 1000);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function json(value, status = 200) {
  return new Response(JSON.stringify(value), {
    status,
    headers: {
      "Content-Type": "application/json;charset=UTF-8",
      "Cache-Control": "no-store",
    },
  });
}

const testHelpers = {
  classifyKomerzaForDelivery,
  checkoutMatchesStoredOrder,
  convertToUsdMinor,
  customerEmailFromOrder,
  isSkinloopTestEvent,
  normalizeCustomerEmail,
  renewDeliveryLease,
  shouldEnqueueFulfillment,
  verifySkinloopSignature,
  verifySkinloopSignatureWithRotation,
  webhookAmountMinor,
  webhookMatchesOrder,
  eventStatusAgrees,
  terminalAuthoritativeStatus,
  shouldRetryAuthoritativeStatus,
  rejectWebhook,
  transitionPaymentStatus,
  outboxPublishable,
};

worker.__test = testHelpers;

export default worker;
