import test from "node:test";
import assert from "node:assert/strict";
import worker from "../src/worker.ts";
import { readFileSync } from "node:fs";

const helpers = (worker as any).__test;

test("runtime rejects event type/status mismatches", () => {
  assert.equal(helpers.eventStatusAgrees("payment.pending", "completed"), false);
  assert.equal(helpers.eventStatusAgrees("payment.completed", "completed"), true);
  assert.equal(helpers.eventStatusAgrees("payment.reverted", "reverted"), true);
});

test("runtime monotonic helper does not permit nonterminal regression", () => {
  assert.equal(helpers.transitionPaymentStatus("completed", "pending"), "completed");
  assert.equal(helpers.transitionPaymentStatus("reverted", "completed"), "reverted");
  assert.equal(helpers.transitionPaymentStatus("completed", "reverted"), "reverted");
});

test("rejection logger metadata cannot contain body fields", () => {
  // rejectWebhook only serializes categorical error/status/eventId metadata.
  // This source-level guard protects the raw-body non-logging invariant.
  assert.doesNotMatch(String(helpers.rejectWebhook || ""), /rawBody|rawText/);
});

test("scheduled outbox selection excludes published and active leases", () => {
  const now = Date.now();
  assert.equal(helpers.outboxPublishable({ published_at: null, publisher_lease_until: null }, now), true);
  assert.equal(helpers.outboxPublishable({ published_at: "2025-01-01T00:00:00Z" }, now), false);
  assert.equal(helpers.outboxPublishable({ published_at: null, publisher_lease_until: new Date(now + 60_000).toISOString() }, now), false);
  assert.equal(helpers.outboxPublishable({ published_at: null, publisher_lease_until: new Date(now - 60_000).toISOString() }, now), true);
});

test("migration declares publisher lease columns on outbox, not orders", () => {
  const sql = readFileSync(new URL("../migrations/0001_initial.sql", import.meta.url), "utf8");
  const outbox = sql.slice(sql.indexOf("CREATE TABLE IF NOT EXISTS skinloop_outbox"), sql.indexOf("CREATE INDEX"));
  assert.match(outbox, /publisher_owner/);
  assert.match(outbox, /publisher_lease_until/);
  const orders = sql.slice(0, sql.indexOf("CREATE TABLE IF NOT EXISTS skinloop_webhook_events"));
  assert.doesNotMatch(orders, /publisher_owner|publisher_lease_until/);
});

test("terminal failure states are non-deliverable and cannot return to completed", () => {
  for (const terminal of ["failed", "declined", "expired", "canceled", "cancelled"]) {
    assert.equal(helpers.transitionPaymentStatus("pending", terminal), terminal);
    assert.equal(helpers.transitionPaymentStatus(terminal, "completed"), terminal);
  }
  assert.equal(helpers.transitionPaymentStatus("reconciliation_required", "completed"), "reconciliation_required");
});

test("stale completed updates cannot revive any persisted terminal state", () => {
  for (const terminal of ["reverted", "reconciliation_required", "failed", "declined", "expired", "canceled", "cancelled"]) {
    assert.equal(helpers.transitionPaymentStatus(terminal, "completed"), terminal);
    assert.equal(helpers.transitionPaymentStatus(terminal, "pending"), terminal);
  }
});