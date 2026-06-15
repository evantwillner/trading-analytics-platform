
Real-Time Trading Analytics Platform

A real-time trading analytics platform demonstrating modern fintech architecture patterns including gRPC streaming, WebSocket fan-out, Kafka-based event pipelines, Go services, and cloud-ready infrastructure via Terraform.

This project is built incrementally to mirror production trading systems.

Architecture Overview

High-level data flow:

Go Market Data Producer
→ Kafka topic (ticks.v1)
→ C# AnalyticsService (Kafka consumer + gRPC server)
→ WebSocket Gateway (Go or C#)
→ React / Electron dashboard

Responsibilities:
  Go Producer
    - Generates and publishes tick data to Kafka.

  Kafka (Redpanda locally)
    - Acts as the streaming backbone for event-driven data flow.

  C# AnalyticsService
    - Consumes Kafka messages and exposes strongly-typed gRPC endpoints, including server-streaming APIs.

  WebSocket Gateway
    - Bridges gRPC streaming data to browser/Electron clients via WebSockets with per-client subscription filtering.

  React / Electron (planned)
    - Renders live streaming market data.

Technology Stack:
Backend

  - .NET 8 (C#) — gRPC services
  - Go — high-concurrency streaming services
  - Kafka (Redpanda locally) — event backbone
  - WebSockets — real-time UI delivery

Infrastructure

  - Docker Compose — local development stack
  - Terraform (planned) — AWS deployment

  Target AWS services:
   - ECS Fargate
   - RDS Postgres
   - MSK (Kafka)
   - CloudWatch

    Application Load Balancer

Repository Structure:

trading-analytics-platform/
  services/
    AnalyticsService/ (C# gRPC service)

  services-go/
    marketdata-producer/ (Go Kafka producer)
    websocket-gateway/ (Go gRPC → WebSocket bridge)

  proto/
    analytics.proto (gRPC contract)

  infra/
    docker-compose.yml (Kafka + UI)

  apps/ (planned)
    dashboard-web
    dashboard-electron

Start Kafka (Redpanda)

From repository root: docker compose -f infra/docker-compose.yml up -d

Kafka broker: localhost:9092
Kafka UI: http://localhost:8080

Create Topic:
```
docker exec -it redpanda rpk topic create ticks.v1
docker exec -it redpanda rpk topic list
```
Run C# Analytics Service:
```
dotnet run --project services/AnalyticsService
```
Exposes:

GetSnapshot (unary gRPC)

StreamTicks (server-streaming gRPC)

Run Go Market Data Producer (skeleton)
cd services-go/marketdata-producer
go run .
Core Concepts Demonstrated
- Proto-first API design
- Strongly-typed gRPC contracts
- Server-streaming RPC
- Event-driven architecture via Kafka
- WebSocket fan-out with per-client subscription state
- Multi-language backend architecture (C# + Go)
- Cloud-ready infrastructure planning (Terraform + AWS)

Milestones:

Phase 0 – gRPC Foundation
- Define analytics.proto
- Implement C# gRPC service
- Implement server-streaming tick feed

Phase 1 – Kafka + Go Foundation
- Add local Kafka-compatible broker (Redpanda)
- Create ticks.v1 topic
- Add Go service skeleton

Phase 2 – Go Producer
- Publish tick events to Kafka
- Validate messages via Kafka UI

Phase 3 – C# Kafka Consumer
- Replace fake tick generator with Kafka consumer
- Maintain latest tick state
- Continue exposing gRPC stream

Phase 4 – Go WebSocket Gateway
- gRPC client in Go
- Per-client subscription filtering
- High-concurrency WebSocket handling

Phase 5 – React + Electron
- Real-time watchlist UI
- Virtualized table
- Desktop packaging

Phase 6 – AWS Deployment (Terraform)
- ECS Fargate
- RDS Postgres
- MSK
- CloudWatch
- ALB

Project Goal:

This project is designed to demonstrate real-time distributed system design using technologies commonly found in fintech environments. It emphasizes:

- Streaming data pipelines
- Strong service boundaries
- Concurrency models in both C# and Go
- Infrastructure-as-code deployment readiness