# Payment Orchestrator

A pre-checkout, metadata-only payment orchestrator that sits between checkout
and multiple downstream Payment Gateways (PGs). It never processes a payment
itself — it decides **which** gateway a session should go to, hands the user
a redirect URL, and then tracks that gateway's health, cost, and eventual
settlement outcome, so a single gateway's outage or rate-limit never means a
lost sale.

Built around a concrete failure mode: during peak traffic, a single primary
gateway with a hard capacity ceiling collapsed under load it couldn't
sustain, while a naive multi-PG split would have destroyed margins on a
channel where the primary gateway charged 0% MDR. This service adds
health-aware, cost-aware, capacity-aware routing with safe, verified
failover — without ever double-charging a customer.

## What this service stores — and doesn't

**Stores (in Redis):** ASR sliding-window outcome counts, health-probe
results, order/attempt metadata (order id, gateway id, idempotency key,
cost, status), distributed-lock state, and per-gateway TPS counters.

**Never stores:** card numbers, CVVs, UPI PINs, OTPs, or the raw
`userSessionPayload` handed to a gateway. That payload lives in memory for
the duration of a single request/attempt and is discarded — see
`orchestrator/processPayment.js`, which never passes it into `orderStore.js`.

## Architecture

```
checkout page                                    gateway's own hosted page
      │  user selects instrument                          ▲
      ▼                                                    │ user redirected here
POST /v1/payments                                           to actually pay
      │
      ▼
┌──────────────────────────────────────────────────────────────────────┐
│  orchestrator/processPayment.js                                       │
│                                                                        │
│  1. routing/selectGateway.js                                          │
│       - filter gateways by channel support                            │
│       - filter by health: ASR (Bayesian-smoothed) >= 80% + live probe │
│       - rank remaining candidates by cost                             │
│       - push any candidate at its TPS capacity cap to the back        │
│       -> ranked candidate list (primary + fallback-ordered)           │
│                                                                        │
│  2. adapter.initiate() on the top candidate                           │
│       - INITIATED   -> return { redirectUrl }. NOT a final outcome —  │
│                         settlement is confirmed later via webhook.    │
│       - USER_ERROR  -> fail immediately, never cascade                │
│       - PROVIDER_ERROR (timeout/5xx) -> do NOT blindly cascade:       │
│           acquire a distributed lock on the order, then run a         │
│           synchronous out-of-band checkStatus() against the SAME      │
│           gateway first                                               │
│             - confirms SUCCESS -> order complete, cancel failover     │
│             - confirms FAILED/PENDING/NOT_FOUND -> safe to cascade    │
│           to the next candidate, with its own scoped idempotency key  │
└──────────────────────────────────────────────────────────────────────┘
      │
      ▼ (asynchronously, later)
POST /v1/webhooks/:gatewayId  ── gateway confirms real settlement
      │
      ▼
┌──────────────────────────────────────────────────────────────────────┐
│  orchestrator/reconciliation.js                                       │
│    - feeds the confirmed outcome back into that gateway's ASR window  │
│    - detects if an order now has MORE THAN ONE successful capture     │
│      (the gateway we "gave up on" during verification turns out to    │
│      have secretly settled after all)                                 │
│    - auto-refunds every successful attempt except the first-confirmed │
│      winner, logging an audit-trail event for each refund             │
└──────────────────────────────────────────────────────────────────────┘
```

A background job (`health/healthProbe.js`) independently pings each
gateway's health endpoint every 60s and writes latency/uptime into Redis, so
a gateway can be marked unhealthy even between transactions.

### Project structure

```
src/
  health/         ASR engine (Bayesian smoothing), Redis sliding-window state, synthetic probe
  routing/        cost config, selectGateway (channel + health + cost + capacity), TPS tracking
  orchestrator/   processPayment (redirect + verified failover), idempotency keys,
                  distributed lock, order/attempt store, reconciliation worker
  api/            Fastify routes + app/server bootstrap
  adapters/       provider-agnostic initiate/checkStatus/refund/probe interface (mock + HTTP)
  events/         SQS / in-memory mock event publisher
  config/         all env-var configuration in one place
mock-servers/     standalone mock PG HTTP server (used by docker-compose)
test/             Jest unit + integration tests
deploy/           ECS task definition sketch
```

## How the ASR (Adjusted Success Rate) engine works

```
ASR = Successful Transactions / (Total Attempts − User Errors)
    = Successful / (Successful + Provider Errors)
```

`USER_ERROR` (wrong UPI PIN, expired OTP, insufficient funds) is excluded
from the denominator entirely — a gateway shouldn't be marked unhealthy
because users keep entering the wrong PIN against it. Only `PROVIDER_ERROR`
(timeout, 5xx, gateway-side failure) counts against a gateway.

**Bayesian smoothing / default weighting for cold-start gateways:** real
counts are blended with a virtual pseudo-count prior — by default 48
successes / 2 failures, i.e. a 96% success rate over a virtual
50-transaction seed window:

```
ASR = (priorSuccess + successes) / (priorSuccess + priorFailure + successes + providerErrors)
```

This means a brand-new gateway starts at exactly 96% (not "unknown," not
0%), a single early real failure barely moves the score
(`48/51 ≈ 94.1%`, not `0/1 = 0%`), and as real volume accumulates the
prior's influence dilutes away and ASR converges to the gateway's true
observed rate. `ASR_PRIOR_SUCCESS` / `ASR_PRIOR_FAILURE` are configurable —
and are deliberately kept *above* `ASR_HEALTHY_THRESHOLD`, not sitting
right on it: if the prior mean equaled the threshold exactly, a single real
failure would always push it below (the denominator grows, the numerator
doesn't), so a cold-start gateway would fail its very first real hiccup.

Each gateway keeps a sliding window of its last 1000 transactions in Redis:
a Redis list maintains window order/eviction, and a Redis hash keeps running
counts in sync via a Lua script, so `getGatewayHealth()` is an O(1) hash
read — comfortably under the 5ms target.

A gateway is eligible for routing when ASR ≥ `ASR_HEALTHY_THRESHOLD`
(default 0.92) **and** the live synthetic probe hasn't flagged it
unreachable — probe-down always wins regardless of ASR.

## How routing works

`routing/costConfig.js` holds a `(gateway, channel) -> MDR or flat fee`
table, updatable at runtime via `POST /v1/gateways/config` — no redeploy.

`selectGateway(redis, channel, amount)`:
1. filters to gateways that support the channel
2. filters to gateways with ASR ≥ threshold and a reachable probe
3. ranks the remainder by computed cost, cheapest first
4. pushes any candidate currently at/over its configured TPS cap
   (`ROUTING_MAX_TPS_BY_GATEWAY`) to the back of the list — still a valid
   fallback, just not first choice, so a 0%-MDR primary gets a safe
   throughput buffer instead of collapsing under load it can't sustain
5. returns a **typed result**: `{ ok: true, candidates: [...] }` with the
   full fallback-ordered list, or `{ ok: false, reason: 'NO_HEALTHY_GATEWAY' }`
   — callers never need to catch a routing exception

## How verified failover works

`orchestrator/processPayment.js`:
1. calls `adapter.initiate()` on the top candidate, with its own idempotency
   key (`ORD_1001_gatewayA_TRY1`)
2. **`INITIATED`** → returns the redirect URL. This confirms a checkout
   session was created — **not** that the payment succeeded. Real
   settlement is confirmed later, asynchronously, via webhook.
3. **`USER_ERROR`** → fails immediately. A different gateway can't fix a
   wrong PIN or bad input, so there's no cascade.
4. **`PROVIDER_ERROR`** (timeout/5xx) → the orchestrator does **not**
   assume failure and blindly retry. In a redirect flow, a timeout on our
   side doesn't mean the gateway didn't process the request. Instead:
   - acquires a Redis distributed lock on the order
     (`orchestrator/distributedLock.js` — `SET NX PX` + Lua
     compare-and-delete release)
   - issues a synchronous out-of-band `checkStatus()` call to the **same**
     gateway
   - if that confirms `SUCCESS` → the order is marked complete with that
     gateway and failover is cancelled entirely
   - if it confirms `FAILED` / `PENDING` / `NOT_FOUND` → only then does the
     orchestrator cascade to the next candidate, with a fresh
     attempt-scoped idempotency key (`ORD_1001_gatewayB_TRY2`) — this key
     scoping means a late-recovering earlier attempt can **never** collide
     with or re-trigger a later one
5. cascades cap at `FAILOVER_MAX_CASCADES` (default 2, so 3 attempts total)

## The safety net: async reconciliation

Even with pre-failover verification, one race remains: gateway A's status
check comes back `PENDING`/`NOT_FOUND`, the orchestrator correctly cascades
to gateway B, B succeeds — but 30 seconds later gateway A's session (which
really was created) finishes settling too. Both gateways send an async
webhook.

`orchestrator/reconciliation.js` (triggered by every incoming webhook):
1. applies the webhook's status to the matching attempt, feeding the
   outcome back into that gateway's ASR window (skipping already-terminal
   attempts, so a duplicate webhook delivery never double-counts)
2. checks whether the order now has **more than one** successful capture
3. if so, keeps the first-confirmed winner and calls `adapter.refund()` on
   every other successful attempt, updating its status to `REFUNDED` and
   publishing a `RECONCILED_REFUND` audit-trail event
4. runs under the same per-order distributed lock as pre-failover
   verification, so a webhook arriving mid-cascade can't race the
   orchestrator's own read-modify-write of the order

This was verified end-to-end over real HTTP with two independent gateway
processes — see "Running locally" below for the exact reproduction steps.

## Event log

Every stage of the orchestration flow is published as an event (to SQS in
production, or the in-memory mock publisher for local dev/tests — see
`events/eventPublisher.js`), so routing decisions, retries, and outcomes are
all auditable after the fact:

| Event | Published when |
|---|---|
| `ATTEMPT_STARTED` | an `initiate()` call is about to be dispatched to a candidate gateway |
| `REDIRECTED` | `initiate()` succeeded — a checkout session + redirect URL were created |
| `ATTEMPT_FAILED` | `initiate()` returned `USER_ERROR` or `PROVIDER_ERROR` |
| `CASCADED` | a `PROVIDER_ERROR` was verified as a genuine failure and the orchestrator is moving to the next candidate |
| `VERIFIED_RECOVERED` | pre-failover verification found the "timed out" gateway had actually succeeded — failover was cancelled |
| `WEBHOOK_RECEIVED` | an async settlement confirmation arrived from a gateway |
| `RECONCILED_REFUND` | reconciliation detected a double-capture and auto-refunded the late-settling attempt |
| `FINAL_OUTCOME` | the order reached a terminal state — success, `USER_ERROR`, `NO_HEALTHY_GATEWAY`, or `ALL_CANDIDATES_EXHAUSTED` |

## API

- `POST /v1/payments` — `{ channel, amount, currency, userSessionPayload }` → `{ ok, orderId, gatewayId, idempotencyKey, redirectUrl }`. Confirms a session was created, not that payment succeeded.
- `POST /v1/webhooks/:gatewayId` — `{ idempotencyKey, status: 'SUCCESS'|'FAILED', reason? }` → applies settlement + runs reconciliation. **A production deployment must verify each provider's webhook signature before trusting this payload** — omitted here since it's provider-specific.
- `GET /v1/orders/:orderId` — order + per-attempt status (metadata only, never card data)
- `GET /v1/gateways/health` — current ASR/status/probe latency for every configured gateway
- `GET /v1/gateways/config` / `POST /v1/gateways/config` — read/update the routing cost table at runtime

## Running locally

### Option A: `npm start` only (fastest, no Docker)

Uses in-process mock adapters — no network calls — good for exercising
routing/failover/reconciliation logic without extra infrastructure beyond
Redis.

```bash
npm install
redis-server &
cp .env.example .env
npm start                 # listens on :3000
```

```bash
curl -X POST localhost:3000/v1/payments \
  -H 'Content-Type: application/json' \
  -d '{"channel":"UPI","amount":5000,"currency":"INR","userSessionPayload":{"token":"abc"}}'
# -> { ok, orderId, gatewayId, idempotencyKey, redirectUrl }

curl -X POST localhost:3000/v1/webhooks/<gatewayId> \
  -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"<idempotencyKey from above>","status":"SUCCESS"}'

curl localhost:3000/v1/orders/<orderId>
curl localhost:3000/v1/gateways/health
```

### Option B: full stack via docker-compose

Spins up the app + Redis + localstack (mock SQS) + 3 standalone mock PG
servers over real HTTP, so failover and reconciliation are exercised over
real network calls and real timing, not in-process shortcuts.

```bash
docker compose up --build
```

Each mock PG server exposes a live control endpoint to simulate outages,
ghost-accepted sessions, and late settlement without restarting the
container — this is exactly how the double-capture reconciliation scenario
above was reproduced and verified:

```bash
# 1. Configure gatewayB (mapped to :4002) to "time out" on us while secretly
#    accepting the session, and take 60s to settle:
curl -X POST localhost:4002/control -H 'Content-Type: application/json' \
  -d '{"failureRate":1,"ghostAcceptOnTimeout":true,"settleDelayMs":60000}'

# 2. Send a payment — it cascades to the next-cheapest gateway, since
#    verification finds gatewayB's session still PENDING:
curl -X POST localhost:3000/v1/payments -H 'Content-Type: application/json' \
  -d '{"channel":"UPI","amount":5000,"currency":"INR","userSessionPayload":{"token":"abc"}}'

# 3. Confirm the cascaded gateway's normal settlement webhook.

# 4. Force gatewayB's session to settle immediately (simulating it secretly
#    completing 30s later), then fire ITS webhook too:
curl -X POST localhost:4002/control -H 'Content-Type: application/json' \
  -d '{"forceSettleKey":"<gatewayB idempotencyKey>","forceSettleOutcome":"SUCCESS"}'
curl -X POST localhost:3000/v1/webhooks/gatewayB -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"<gatewayB idempotencyKey>","status":"SUCCESS"}'

# 5. GET /v1/orders/:orderId — exactly one attempt is SUCCESS, the other is
#    REFUNDED, and the order is COMPLETE.
```

## Testing

```bash
npm test
```

61 tests across:
- `test/asrEngine.test.js` — Bayesian smoothing (cold-start prior, dilution
  as real volume grows), user-error exclusion, sliding-window eviction,
  80% eligibility cutoff
- `test/selectGateway.test.js` — least-cost selection, health filtering,
  capacity-aware overflow, typed no-healthy-gateway results
- `test/idempotency.test.js` — key scoping and collision-safety across
  cascaded attempts
- `test/distributedLock.test.js` — acquire/release semantics, compare-and-
  delete safety, release-on-throw
- `test/failover.test.js` — redirect-flow happy path, pre-failover
  verification (both the "genuinely failed, cascade" and "actually
  succeeded, don't cascade" branches), cascade caps, USER_ERROR
  non-cascading, and the double-capture reconciliation/auto-refund scenario
- `test/api.test.js` — HTTP-level request validation, webhook handling, and
  a dedicated assertion that card details never appear in stored/returned
  order data

## Configuration

All configuration is environment-variable driven — see `.env.example` for
the full list (Redis URL, SQS queue URL/endpoint, gateway HTTP endpoints,
ASR prior/threshold, probe interval, TPS caps, lock TTL, retry caps).
Nothing is hardcoded.

## Deployment

`Dockerfile` builds a production image; `deploy/ecs-task-definition.sketch.json`
is a starting point for an ECS Fargate task definition (fill in
account/region/ARNs before applying — it's a sketch, not a validated
artifact). The app itself is stateless — all health/order state lives in
Redis — so it scales horizontally behind a load balancer; use `GET /healthz`
as the target-group health check, and expose `POST /v1/webhooks/:gatewayId`
behind the same ALB (or a dedicated listener) for gateways to call back.

## Known simplifications

- Webhook signature verification is not implemented (provider-specific) —
  a production deployment must verify it before trusting the payload.
- The distributed lock is a single-Redis-instance `SET NX PX` pattern, not
  full Redlock across a cluster — sufficient for serializing two internal
  workers on one order, not for a use case with a higher correctness bar.
- The cost config is in-memory (resets on restart, not shared across
  replicas). The interface is designed to be swapped for a DB or shared
  cache without touching `selectGateway()`.
- Reconciliation always keeps the *first-confirmed* successful attempt as
  the winner; a real system might instead prefer the earliest-created
  attempt regardless of confirmation order, depending on dispute/chargeback
  policy — this is a policy choice, not a technical constraint.
