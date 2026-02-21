# Real-Time Trading Analytics Platform (C# / gRPC / WebSockets / React / Electron)

This repo is a portfolio project designed to mirror a modern fintech “streaming analytics platform” architecture:
- Internal service streaming via **gRPC**
- UI delivery via **WebSockets**
- Real-time dashboard in **React**
- Desktop container via **Electron**
- A supporting **Ledger Service** for correctness (positions, fills, PnL)

---

## Architecture (High Level)

**MarketDataSimulator** → emits ticks/trades (fake exchange feed)  
**AnalyticsService (gRPC)** → computes metrics + serves server-streaming feeds  
**WebSocketGateway** → bridges gRPC streams → WebSocket clients  
**Dashboard (React)** → renders live market data + analytics efficiently  
**Dashboard (Electron)** → desktop container for the same React app  
**LedgerService (gRPC)** → supporting service for financial correctness (positions/PnL)

---

## Services & Apps

### Backend
- `services/MarketDataSimulator`  
  Generates deterministic fake tick/trade events for symbols.

- `services/AnalyticsService` (gRPC)  
  Owns analytics computations and exposes:
  - Unary snapshot endpoints
  - Server-streaming endpoints for real-time feeds

- `services/WebSocketGateway`  
  WebSocket server that:
  - Accepts client subscriptions (symbols)
  - Subscribes to internal gRPC streams
  - Pushes updates to clients (JSON messages)

- `services/LedgerService`  
  Supporting system-of-record style service:
  - Fills, positions, PnL snapshots
  - Queried by analytics for “position-aware” metrics

### Frontend
- `apps/dashboard-web`  
  React + TypeScript real-time dashboard.

- `apps/dashboard-electron`  
  Electron wrapper around the dashboard.

---

## Streaming Design Notes (Important)

### Why gRPC internally + WebSockets to the UI?
- gRPC is strong for service-to-service communication, typed contracts, streaming.
- Browsers/Electron UIs commonly consume streaming data via WebSockets.
- The WS gateway is a realistic “edge fan-out” pattern.

### Performance approach (UI)
- Messages can be extremely frequent.
- The UI should not re-render on every message.
- Buffer updates and apply them on a fixed cadence (e.g. 30–60 FPS).
- Large tables should be virtualized.

---

## Project Milestones (Changelog)

### Milestone 0: Repo skeleton + proto contracts
- [x] Add `analytics.proto`
- [x] gRPC service boots

### Milestone 1: WebSocket Gateway
- [x] WebSocket endpoint: `/ws/marketdata`
- [x] gRPC → WebSocket bridge (StreamTicks fan-out as JSON)
- [x] Per-client subscriptions: subscribe/unsubscribe/set_symbols
- [x] Verified delivery + filtering using `wscat`


### Milestone 3: React dashboard
- [ ] Live watchlist table
- [ ] Throttled rendering + buffering

### Milestone 4: Electron container
- [ ] Desktop packaging
- [ ] Same WS feed

### Milestone 5: Ledger integration
- [ ] LedgerService positions/PnL
- [ ] Analytics calls ledger for position-aware analytics

---

# Helpful Notes

1 - You can use grpcurl to validate network & streaming behavior without the need of a client

grpcurl -plaintext \
  -import-path ./proto \
  -proto analytics.proto \
  -d '{"symbol":"AAPL"}' \
  localhost:5226 \
  analytics.v1.AnalyticsService/GetSnapshot

grpcurl -plaintext \
  -import-path ./proto \
  -proto analytics.proto \
  -d '{"symbols":["AAPL","MSFT"]}' \
  localhost:5226 \
  analytics.v1.AnalyticsService/StreamTicks


