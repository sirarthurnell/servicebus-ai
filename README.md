# Service Bus + AI worker (.NET)

> Work in progress.

Event-driven sample: two .NET microservices communicating over Azure Service Bus. A publisher emits events to a topic; a subscriber reacts to them. The subscriber is an AI worker that processes each event through a local LLM (Ollama) via Microsoft Agent Framework, built on `IChatClient`.

The whole thing runs locally at no cost using the official Azure Service Bus emulator.

## Stack

- .NET 10
- Azure Service Bus (local emulator, backed by SQL Server)
- `Azure.Messaging.ServiceBus`
- Microsoft Agent Framework / `Microsoft.Extensions.AI` with OllamaSharp (local Ollama)
- Docker / Docker Compose

## Target architecture

- A **topic** with multiple **subscriptions** (fan-out): each subscription receives its own copy of every event.
- **Competing consumers** within a subscription for load distribution.
- **Peek-lock** delivery with explicit completion, dead-lettering, and idempotent handling.
- One subscription drives an **AI agent** that classifies/summarizes events and can invoke tools.

## Status

In place:
- Local Service Bus emulator (Docker Compose: emulator + SQL Server backend).
- Declarative topology (`Config.json`): one topic, two subscriptions.
- A console spike demonstrating fan-out across subscriptions and competing consumers within a single subscription.

In progress:
- Publisher and subscriber microservices (peek-lock, `Complete`/`Abandon`, dead-lettering, idempotency).
- AI worker (Microsoft Agent Framework + local Ollama).

## Run locally

Requires Docker Desktop.

```bash
cp .env.example .env   # then set a strong MSSQL_SA_PASSWORD
docker compose up -d
curl http://localhost:5300/health
```

The emulator exposes AMQP on `5672` and a management/health endpoint on `5300`.

---

*This README is provisional and will be replaced once the project is complete.*
