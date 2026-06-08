# vLLM Forwarder

A FastAPI-based reverse proxy that sits in front of multiple [vLLM](https://github.com/vllm-project/vllm) inference servers and exposes a unified, OpenAI-compatible API. Routes chat completions, speech/TTS, and arbitrary endpoints to the correct backend based on model or voice name.

## Quick Start

```bash
docker compose up -d --build
```

The proxy listens on port **5000**. Swagger UI at `/docs`.

## Local Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python source/uvicorn.main.py
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `API_HOST` | `127.0.0.1` | Bind address |
| `API_PORT` | `5000` | Listen port |
| `WORKER_NUM` | `1` | Uvicorn worker processes |

Set via `.env` file or environment variables.

## Usage

### Register models

```bash
curl -X POST http://localhost:5000/model \
  -H "Content-Type: application/json" \
  -d '{"url": "http://192.168.1.50:8000/v1"}'
```

### Register voices

```bash
curl -X POST http://localhost:5000/voice \
  -H "Content-Type: application/json" \
  -d '{"url": "http://192.168.1.50:8091/v1"}'
```

### Chat completion

```bash
curl -X POST http://localhost:5000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "meta-llama/Llama-3-8b", "messages": [{"role": "user", "content": "Hello!"}]}'
```

### Text-to-speech

```bash
curl -X POST http://localhost:5000/v1/audio/speech \
  -H "Content-Type: application/json" \
  -d '{"voice": "vivian", "input": "Hello world"}'
```
