# VLLM Forwarder

A FastAPI-based reverse proxy that sits in front of multiple inference servers and exposes a unified, OpenAI-compatible API. Routes chat completions, embeddings, speech/TTS, and arbitrary endpoints to the correct backend based on model name, voice name, or a user-configured forward URL.

Features:
- **Model routing** — register models from vLLM servers and proxy by name
- **Voice routing** — register TTS voices and proxy REST + WebSocket speech
- **Forward URLs** — assign any API key (internal or external) a target URL; all requests with that key route there
- **API key auth** — create user and agent API keys with role-based access
- **Authless mode** — disable auth entirely for development
- **Dual storage** — SQLite by default, optional MongoDB
- **Redis caching** — optional read-through cache for all lookups
- **Usage logging** — chat completions logged to Kafka or rotating JSONL file
- **Multi-tenant** — `NAME_PREFIX` scopes all data, allowing shared infrastructure

## Quick Start

```bash
docker compose up -d --build
```

The proxy listens on port **5000**. Swagger UI at `/docs`.

## Local Development

```bash
python -m venv .venv && source .venv/bin/activate   # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
python source/uvicorn.main.py
```

## Configuration

All variables set via `.env` file or environment variables.

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `API_HOST` | `127.0.0.1` | No | Bind address |
| `API_PORT` | `5000` | No | Listen port |
| `WORKER_NUM` | `1` | No | Uvicorn worker processes |
| `FORWARDED_ALLOW_IPS` | `127.0.0.1` | No | Trusted proxy IPs |
| `NAME_PREFIX` | — | **Yes** | Prefix scoping all data (use different values for dev/prod) |
| `AUTH_URL` | `""` | Only if `AUTHLESS_MODE=false` | URL called by `POST /apikey` to verify user credentials |
| `SECRET_KEY` | `""` | Only if `AUTHLESS_MODE=false` | Secret for hashing API keys |
| `AUTHLESS_MODE` | `false` | No | Disable all API key auth |
| `MONGO_URI` | — | No | MongoDB connection string (replaces SQLite) |
| `MONGO_DB` | — | No | MongoDB database name |
| `KAFKA_BOOTSTRAP_SERVERS` | — | No | Kafka brokers for usage logging |
| `KAFKA_TOPIC` | — | No | Kafka topic for usage logs |
| `USE_REDIS` | `false` | No | Enable Redis caching |
| `REDIS_HOST` | `127.0.0.1` | No | Redis host |
| `REDIS_PORT` | `6379` | No | Redis port |
| `REDIS_DB` | `0` | No | Redis database number |
| `REDIS_AUTH` | — | No | Redis password |
| `REDIS_TIMEOUT` | — | No | Redis socket timeout |

### Authless mode

Set `AUTHLESS_MODE=true` to skip all API key checks. Model and voice management endpoints work without auth. `/forward` still requires a Bearer token. `/apikey` and `/agent` endpoints return 404. `AUTH_URL` and `SECRET_KEY` are not required.

## API Endpoints

### OpenAI Standard

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/v1/models` | Optional¹ | List models |
| `POST` | `/v1/chat/completions` | Required¹ | Proxy chat completions |
| `GET` | `/v1/audio/voices` | Optional¹ | List voices |
| `POST` | `/v1/audio/speech` | Required¹ | Proxy REST TTS |
| `WS` | `/v1/audio/speech/stream` | None | Proxy WebSocket speech streaming |

¹ Optional: token is not required. Required: Bearer header must be present but may be random key. If a registered API key with a forward URL is provided, proxies to that URL instead of the local database.

### Model Management

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/model` | User key² | Register models from a vLLM server |
| `DELETE` | `/model/{name}` | User key² | Remove a model route |

### Voice Management

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/voice` | User key² | Register voices from a TTS server |
| `DELETE` | `/voice/{name}` | User key² | Remove a voice route |
| `DELETE` | `/voice/url/{url}` | User key² | Remove all voices by URL |

² Only required when `AUTHLESS_MODE=false`. In authless mode these endpoints work without a key.

### Forward Management

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/forward` | Any key³ | Set a forward URL for your API key. Probes `{url}/models` before saving. |
| `DELETE` | `/forward` | Any key³ | Remove the forward URL for your API key |

³ Any Bearer token works — internal (`pr-`, `ag-`) or external (`sk-`, `sk-or-v1-`). The key itself becomes the lookup key for forwarding.

### Authentication

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/apikey` | None⁴ | Create a new user API key. Body forwarded to `AUTH_URL`. |
| `GET` | `/apikey` | Any key | Retrieve data for your API key (user, agent, or forward key). |

⁴ No API key needed; the endpoint forwards the request body to `AUTH_URL` for credential verification.

### Agent Management

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/agent` | User key | Generate a new agent API key |
| `DELETE` | `/agent` | User key | Delete an agent API key |

### Catch-all

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `*` | `/{path}` | Optional¹ | Routes by `model` in payload body; with forward URL, routes all traffic |

## Usage

### Authless mode (development)

```bash
# .env
NAME_PREFIX=dev
AUTHLESS_MODE=true
```

### Register models

```bash
curl -X POST http://localhost:5000/model \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer pr-xxx" \
  -d '{"url": "http://192.168.1.50:8000/v1"}'
```

### Register voices

```bash
curl -X POST http://localhost:5000/voice \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer pr-xxx" \
  -d '{"url": "http://192.168.1.50:8091/v1", "extra_kwargs": ["speed", "pitch"]}'
```

### Chat completion (local model)

```bash
curl -X POST http://localhost:5000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer " \
  -d '{"model": "meta-llama/Llama-3-8b", "messages": [{"role": "user", "content": "Hello!"}]}'
```

### Chat completion (via forward URL)

```bash
# 1. Set a forward URL for your OpenRouter key
curl -X POST http://localhost:5000/forward \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-or-v1-xxx" \
  -d '{"url": "https://openrouter.ai/api/v1"}'

# 2. All requests with that key now route to OpenRouter
curl -X POST http://localhost:5000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-or-v1-xxx" \
  -d '{"model": "openai/gpt-4o", "messages": [{"role": "user", "content": "Hello!"}]}'
```

### Text-to-speech

```bash
curl -X POST http://localhost:5000/v1/audio/speech \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer " \
  -d '{"voice": "vivian", "input": "Hello world"}'
```

### Create API keys

```bash
# Create a user key (body depends on what AUTH_URL expects)
curl -X POST http://localhost:5000/apikey \
  -H "Content-Type: application/json" \
  -d '{"email": "user@example.com", "otp": "123456"}'

# Create an agent key
curl -X POST http://localhost:5000/agent \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer pr-xxx" \
  -d '{"agent_name": "my-bot", "agent_description": "A helpful assistant"}'
```

## Architecture

```
Client
  │
  ▼
VLLM Forwarder (:5000)
  │
  ├─ forward URL set?  ──────────▶  External API (OpenAI, OpenRouter, etc.)
  │
  ├─ model in payload?  ─────────▶  vLLM server A (model lookup)
  │
  └─ voice in payload?  ─────────▶  TTS server B (voice lookup)


Storage:  SQLite (default)  or  MongoDB (optional)
Cache:    Redis (optional, read-through, 24h TTL for hits, 60s for misses)
Logging:  Kafka (optional)  or  rotating JSONL file (logs/chat_completions.jsonl)
```

## API Key Types

| Prefix | Type | Created by | Used for |
|--------|------|------------|---------|
| `pr-` | User | `POST /apikey` (via `AUTH_URL`) | Management endpoints, chat, forwarding |
| `ag-` | Agent | `POST /agent` (by a user) | Chat, forwarding (no management access) |
| `sk-` / `sk-or-v1-` | External | N/A | Forward-only (set via `POST /forward`) |
