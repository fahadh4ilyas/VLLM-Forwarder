# app/main.py
import asyncio
import datetime
import glob
import json
import logging
import os
import timeit
import typing

import httpx
import websockets
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends, HTTPException, WebSocket, WebSocketDisconnect, Body
from fastapi.responses import RedirectResponse, Response, StreamingResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from typing import Any, List, Optional

from .config import config
from .database import Base, engine
from .auth_utils import (
    bearer_scheme,
    auth_api_key,
    check_api_key,
    authenticated_auth,
    require_auth,
    require_user_auth,
    generate_account_auth,
    generate_agent_auth,
    delete_agent_auth,
    set_forward_url,
    delete_forward_url,
    resolve_forward_url,
    get_model,
    list_models,
    upsert_models,
    delete_model_by_name,
    get_voice,
    list_voices_all,
    upsert_voices,
    delete_voice_by_name,
    delete_voices_by_url,
)
from .tools import get_kafka_producer, hash_json

LOGGER_ACCESS = logging.getLogger('gunicorn.access')
LOGGER = logging.getLogger('uvicorn.error')

# Global HTTP client for connection pooling
http_client: httpx.AsyncClient = None

# Global Kafka producer
kafka_producer: typing.Any = None
kafka_started: bool = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client, kafka_producer
    # Initialize DB tables
    Base.metadata.create_all(bind=engine)
    # Initialize global async HTTP client
    http_client = httpx.AsyncClient(timeout=httpx.Timeout(None))
    # Initialize Kafka producer if configured
    kafka_producer = get_kafka_producer()
    yield
    # Cleanup on shutdown
    await http_client.aclose()
    if kafka_producer:
        await kafka_producer.stop()


app = FastAPI(
    title='vLLM Forwarder API',
    description='API for managing and proxying requests to multiple vLLM servers with OpenAI-compatible endpoints.',
    version='1.0.0',
    lifespan=lifespan,
)


@app.middleware("http")
async def logging_request(request: Request, call_next):
    client_data = ''
    if request.client:
        client_data = f'{request.client.host}:{request.client.port}'
    LOGGER_ACCESS.info(f'{client_data} - "{request.method.upper()} {request.url.path} {request.url.scheme.upper()}/1.1" START')

    params = str(request.query_params)
    body = await request.body()

    if params:
        LOGGER_ACCESS.info(f'{client_data} - "{request.method.upper()} {request.url.path} {request.url.scheme.upper()}/1.1" PARAMS: {params}')
    if body:
        LOGGER_ACCESS.info(f'{client_data} - "{request.method.upper()} {request.url.path} {request.url.scheme.upper()}/1.1" BODY: {body}')

    start = timeit.default_timer()

    response: Response = await call_next(request)

    if response.headers.get('content-encoding', None) == 'gzip':
        del response.headers['content-encoding']
    response.headers["X-Process-Time"] = f'{timeit.default_timer() - start:.6f}'

    return response


@app.get('/', include_in_schema=False)
async def redirect():
    return RedirectResponse(app.root_path + '/docs')


def _parse_response(text: str):
    """Parse a chat completion response body into a loggable object."""
    stripped = text.strip()
    if stripped.startswith('data:'):
        chunks = []
        for line in stripped.split('\n'):
            line = line.strip()
            if not line.startswith('data:'):
                continue
            payload = line[5:].strip()
            if payload == '[DONE]':
                continue
            try:
                chunks.append(json.loads(payload))
            except json.JSONDecodeError:
                chunks.append(payload)
        return _merge_streaming_chunks(chunks) if chunks else chunks
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return stripped


def _merge_streaming_chunks(chunks: list) -> dict:
    """Merge a list of chat.completion.chunk objects into a single chat.completion-shaped response dict."""
    if not chunks or not isinstance(chunks[0], dict):
        return chunks

    first = chunks[0]
    if first.get('object') != 'chat.completion.chunk':
        return chunks

    merged: dict[str, Any] = {
        'id': first.get('id'),
        'object': 'chat.completion',
        'created': first.get('created'),
        'model': first.get('model'),
    }

    metadata_keys = (
        'system_fingerprint', 'service_tier', 'prompt_routed_experts',
        'prompt_logprobs', 'prompt_token_ids', 'prompt_text',
        'kv_transfer_params', 'usage',
    )
    for chunk in chunks:
        for key in metadata_keys:
            if key in chunk and chunk[key] is not None:
                merged[key] = chunk[key]

    usage = merged.pop('usage', None)

    message: dict[str, Any] = {}
    finish_reason = None

    for chunk in chunks:
        choices = chunk.get('choices', [])
        if not choices:
            continue
        choice = choices[0]
        delta = choice.get('delta', {})

        if 'role' in delta and 'role' not in message:
            message['role'] = delta['role']

        for field in ('content', 'reasoning'):
            if field in delta and delta[field]:
                message[field] = message.get(field, '') + delta[field]

        if 'tool_calls' in delta:
            tc_list = message.setdefault('tool_calls', [])
            for tc in delta['tool_calls']:
                idx = tc.get('index', 0)
                while len(tc_list) <= idx:
                    tc_list.append({})
                for key, val in tc.items():
                    if key == 'index' or val is None:
                        continue
                    if key == 'function':
                        fn = tc_list[idx].setdefault('function', {})
                        for fn_key, fn_val in val.items():
                            if fn_val is None:
                                continue
                            if fn_key == 'arguments':
                                fn['arguments'] = fn.get('arguments', '') + fn_val
                            elif fn_key not in fn:
                                fn[fn_key] = fn_val
                            else:
                                fn[fn_key] = fn_val
                    elif isinstance(val, str):
                        tc_list[idx][key] = (tc_list[idx].get(key, '') + val)
                    else:
                        tc_list[idx][key] = val

        for field in ('refusal', 'annotations', 'audio', 'function_call'):
            if field in delta and delta[field] is not None:
                message[field] = delta[field]

        if choice.get('finish_reason'):
            finish_reason = choice['finish_reason']

    choice_logprobs = first.get('choices', [{}])[0].get('logprobs')
    if 'finish_reason' in first.get('choices', [{}])[0] and not finish_reason:
        finish_reason = first['choices'][0]['finish_reason']

    merged['choices'] = [{
        'index': 0,
        'message': message,
        'logprobs': choice_logprobs,
        'finish_reason': finish_reason,
    }]
    if usage:
        merged['usage'] = usage

    return merged


# ==========================================
# Kafka / File Logging Helper
# ==========================================

_CHAT_LOG_PATH = os.path.join(os.path.abspath(os.getcwd()), 'logs', 'chat_completions.jsonl')
_LOCK_PATH = _CHAT_LOG_PATH + '.lock'
_MAX_BACKUPS = 30


def _lock_file(f):
    try:
        import msvcrt
        msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
    except ImportError:
        import fcntl
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)


def _unlock_file(f):
    try:
        import msvcrt
        msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
    except ImportError:
        import fcntl
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def _rotate_if_needed():
    if not os.path.exists(_CHAT_LOG_PATH):
        return
    file_date = datetime.date.fromtimestamp(os.path.getmtime(_CHAT_LOG_PATH))
    today = datetime.date.today()
    if file_date == today:
        return
    suffix = file_date.isoformat()
    os.rename(_CHAT_LOG_PATH, f'{_CHAT_LOG_PATH}.{suffix}')
    backups = sorted(glob.glob(_CHAT_LOG_PATH + '.*'))
    while len(backups) > _MAX_BACKUPS:
        os.remove(backups.pop(0))


def _write_chat_log_line(line: str):
    """Append a line to the chat log file with locking and daily rotation.

    A lock file serializes the entire critical section (check → rotate → write)
    so that only one worker process can access the log file at a time.
    """
    os.makedirs(os.path.dirname(_CHAT_LOG_PATH), exist_ok=True)
    with open(_LOCK_PATH, 'w') as lock_f:
        _lock_file(lock_f)
        try:
            _rotate_if_needed()
            with open(_CHAT_LOG_PATH, 'a') as log_f:
                log_f.write(line + '\n')
                log_f.flush()
        finally:
            _unlock_file(lock_f)


async def _send_kafka_log(log_data: dict):
    """Send a log entry to Kafka if configured, otherwise write to locked rotating file."""
    global kafka_started
    if config.kafka_bootstrap_servers and config.kafka_topic and kafka_producer:
        try:
            if not kafka_started:
                await kafka_producer.start()
                kafka_started = True
            log_data['_id'] = hash_json(log_data)
            await kafka_producer.send_and_wait(
                config.kafka_topic,
                json.dumps(log_data).encode('utf-8'),
            )
        except Exception:
            LOGGER.warning("Failed to send log to Kafka, falling back to file.")
            _write_chat_log_line(json.dumps(log_data))
    else:
        _write_chat_log_line(json.dumps(log_data))


# ==========================================
# Core Forwarding Engine
# ==========================================

async def forward_request(
    request: Request,
    target_url: str,
    body_bytes: bytes,
    model_name: str
) -> Response:
    """Core engine for forwarding requests to upstream servers."""
    proxy_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in ('host', 'content-length')
    }

    proxy_req = http_client.build_request(
        request.method,
        target_url,
        headers=proxy_headers,
        content=body_bytes,
        params=request.query_params
    )

    send_task = asyncio.create_task(http_client.send(proxy_req, stream=True))
    disconnect_task = asyncio.create_task(request.is_disconnected())

    try:
        while True:
            done, pending = await asyncio.wait(
                [send_task, disconnect_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            if disconnect_task in done:
                is_disconnected = disconnect_task.result()
                if is_disconnected:
                    send_task.cancel()
                    LOGGER_ACCESS.warning(f"Client disconnected early. Cancelling upstream request to '{model_name}'.")
                    return Response(status_code=499)
                else:
                    disconnect_task = asyncio.create_task(request.is_disconnected())
                    continue

            if send_task in done:
                disconnect_task.cancel()
                upstream_response = send_task.result()
                break

    except httpx.RequestError as e:
        LOGGER_ACCESS.error(f"Upstream connection failed for model '{model_name}' at {target_url}: {str(e)}")
        error_payload = {
            "error": {
                "message": f"The upstream server hosting '{model_name}' is currently unreachable.",
                "type": "upstream_server_error",
                "param": None,
                "code": 502
            }
        }
        return JSONResponse(status_code=502, content=error_payload)
    except asyncio.CancelledError:
        return Response(status_code=499)

    response_headers = {
        k: v for k, v in upstream_response.headers.items()
        if k.lower() not in ('content-length', 'content-encoding', 'transfer-encoding', 'connection')
    }

    if upstream_response.is_error:
        await upstream_response.aread()
        try:
            error_data = upstream_response.json()
        except Exception:
            error_data = {"error": {"message": upstream_response.text, "type": "upstream_error", "code": upstream_response.status_code}}
        return JSONResponse(
            status_code=upstream_response.status_code,
            content=error_data,
            headers=response_headers
        )

    async def stream_generator():
        try:
            async for chunk in upstream_response.aiter_raw():
                yield chunk
        finally:
            await upstream_response.aclose()

    return StreamingResponse(
        stream_generator(),
        status_code=upstream_response.status_code,
        headers=response_headers
    )


# ==========================================
# Proxy & Management Endpoints (Text Models)
# ==========================================

class AddModelRequest(BaseModel):
    url: str


@app.post('/model', tags=["Management"])
async def add_model(
    payload: AddModelRequest,
    token: Optional[HTTPAuthorizationCredentials] = Depends(auth_api_key),
):
    """Fetch models from a vLLM server and save them to the DB."""
    auth_data, err = await require_user_auth(token)
    if err:
        return err

    key_id = auth_data.get('api_key', 'anonymous') if auth_data else 'anonymous'
    base_url = payload.url.rstrip('/')
    target_url = f"{base_url}/models"

    headers = {}
    if token and token.credentials:
        headers["Authorization"] = f"Bearer {token.credentials}"

    try:
        response = await http_client.get(target_url, timeout=10.0, headers=headers)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to connect to {target_url}. Error: {str(e)}")

    models_list = data.get("data", [])
    if not models_list:
        raise HTTPException(status_code=400, detail="No models found at the target URL.")

    added_models = await upsert_models(models_list, base_url)
    LOGGER.info(f"POST /model url={base_url} by key={key_id}")
    return {"status": "success", "added_models": added_models}


@app.get('/v1/models', tags=["OpenAI Standard"])
async def get_models(
    request: Request,
    token=Depends(bearer_scheme),
):
    """List all models.

    If an API key with a forward URL is provided, proxies to that URL.
    Otherwise returns models from the local database.
    """
    forward_url, _ = await resolve_forward_url(token)
    if forward_url:
        return await forward_request(request, f"{forward_url.rstrip('/')}/models", b"", "models")

    models = await list_models()
    return {
        "object": "list",
        "data": [m['properties'] for m in models]
    }


@app.delete('/model/{model_name:path}', tags=["Management"])
async def delete_model(
    model_name: str,
    token: Optional[HTTPAuthorizationCredentials] = Depends(auth_api_key),
):
    """Delete a model route from the database."""
    auth_data, err = await require_user_auth(token)
    if err:
        return err

    key_id = auth_data.get('api_key', 'anonymous') if auth_data else 'anonymous'
    LOGGER.info(f"DELETE /model/{model_name} by key={key_id}")

    deleted = await delete_model_by_name(model_name)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found.")
    return {"status": "success", "message": f"Deleted model '{model_name}'"}


# ==========================================
# Proxy & Management Endpoints (Speech API)
# ==========================================

class AddVoiceRequest(BaseModel):
    url: str
    extra_kwargs: List[str] = Field(default_factory=list)


@app.post('/voice', tags=["Speech Management"])
async def add_voice(
    payload: AddVoiceRequest,
    token: Optional[HTTPAuthorizationCredentials] = Depends(auth_api_key),
):
    """Fetch voices from a server and save them to the DB."""
    auth_data, err = await require_user_auth(token)
    if err:
        return err

    key_id = auth_data.get('api_key', 'anonymous') if auth_data else 'anonymous'
    base_url = payload.url.rstrip('/')
    target_url = f"{base_url}/audio/voices"

    headers = {}
    if token and token.credentials:
        headers["Authorization"] = f"Bearer {token.credentials}"

    try:
        response = await http_client.get(target_url, timeout=10.0, headers=headers)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to connect to {target_url}. Error: {str(e)}")

    voices_list = data.get("voices", [])
    if not voices_list:
        raise HTTPException(status_code=400, detail="No voices found at the target URL.")

    added_voices = await upsert_voices(voices_list, base_url, payload.extra_kwargs)
    LOGGER.info(f"POST /voice url={base_url} by key={key_id}")
    return {"status": "success", "added_voices": added_voices}


@app.get('/v1/audio/voices', tags=["Speech Standard"])
async def get_voices(
    request: Request,
    token=Depends(bearer_scheme),
):
    """List all voices.

    If an API key with a forward URL is provided, proxies to that URL.
    Otherwise returns voices from the local database.
    """
    forward_url, _ = await resolve_forward_url(token)
    if forward_url:
        return await forward_request(request, f"{forward_url.rstrip('/')}/audio/voices", b"", "voices")

    voices = await list_voices_all()
    return {"voices": [v['id'] for v in voices], "uploaded_voices": []}


@app.delete('/voice/url/{url:path}', tags=["Speech Management"])
async def delete_voice_by_url(
    url: str,
    token: Optional[HTTPAuthorizationCredentials] = Depends(auth_api_key),
):
    """Delete voices by URL for this prefix."""
    auth_data, err = await require_user_auth(token)
    if err:
        return err

    key_id = auth_data.get('api_key', 'anonymous') if auth_data else 'anonymous'
    LOGGER.info(f"DELETE /voice/url/{url} by key={key_id}")

    count = await delete_voices_by_url(url)
    if count == 0:
        raise HTTPException(status_code=404, detail=f"Voices with URL '{url}' not found.")
    return {"status": "success", "message": f"Deleted {count} voices with URL '{url}'"}


@app.delete('/voice/{voice_name:path}', tags=["Speech Management"])
async def delete_voice(
    voice_name: str,
    token: Optional[HTTPAuthorizationCredentials] = Depends(auth_api_key),
):
    """Delete a voice route for this prefix."""
    auth_data, err = await require_user_auth(token)
    if err:
        return err

    key_id = auth_data.get('api_key', 'anonymous') if auth_data else 'anonymous'
    LOGGER.info(f"DELETE /voice/{voice_name} by key={key_id}")

    deleted = await delete_voice_by_name(voice_name)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Voice '{voice_name}' not found.")
    return {"status": "success", "message": f"Deleted voice '{voice_name}'"}


class SpeechRequest(BaseModel):
    """OpenAI-compatible speech (TTS) request body."""
    voice: str = Field(..., description="Voice ID for routing")

    class Config:
        extra = 'allow'


@app.post('/v1/audio/speech', tags=["Speech Standard"])
async def proxy_audio_speech(
    body: SpeechRequest,
    request: Request,
    token=Depends(bearer_scheme),
):
    """Proxy the REST Speech endpoint to the correct server."""
    body_bytes = await request.body()
    voice_name = body.voice

    forward_url, _ = await resolve_forward_url(token)

    if forward_url:
        target_url = f"{forward_url.rstrip('/')}/audio/speech"
    else:
        db_voice = await get_voice(voice_name)
        if not db_voice:
            raise HTTPException(status_code=404, detail=f"Voice '{voice_name}' not found.")
        target_url = f"{db_voice['url']}/audio/speech"

    response = await forward_request(request, target_url, body_bytes, voice_name)

    if response.status_code == 499:
        LOGGER.warning(f"Client disconnected before upstream response for audio voice={voice_name}")
        return response

    return response


# ==========================================
# Speech WebSocket Endpoint
# ==========================================

@app.websocket("/v1/audio/speech/stream")
async def websocket_speech_stream(websocket: WebSocket):
    await websocket.accept()
    try:
        first_msg_str = await websocket.receive_text()
        first_msg = json.loads(first_msg_str)

        if first_msg.get("type") != "session.config":
            await websocket.close(code=1008, reason="First message must be session.config")
            return

        voice_name = first_msg.get("voice", "vivian")
        db_voice = await get_voice(voice_name)
        if not db_voice:
            await websocket.close(code=1008, reason=f"Voice '{voice_name}' not found")
            return

        allowed_kwargs = db_voice['extra_kwargs'] or []
        user_extra_kwargs = first_msg.pop("extra_kwargs", {})

        if isinstance(user_extra_kwargs, dict):
            for key, val in user_extra_kwargs.items():
                if key in allowed_kwargs:
                    first_msg[key] = val

        base_ws_url = db_voice['url'].replace("http://", "ws://").replace("https://", "wss://")
        target_ws_url = f"{base_ws_url}/audio/speech/stream"

        async with websockets.connect(
            target_ws_url,
            max_size=None,
            ping_interval=None
        ) as upstream_ws:
            await upstream_ws.send(json.dumps(first_msg))

            async def client_to_server():
                try:
                    while True:
                        msg = await websocket.receive_text()
                        await upstream_ws.send(msg)
                except WebSocketDisconnect:
                    LOGGER.warning("WebSocket disconnected by client.")
                    pass

            async def server_to_client():
                try:
                    while True:
                        msg = await upstream_ws.recv()
                        if isinstance(msg, str):
                            await websocket.send_text(msg)
                        else:
                            await websocket.send_bytes(msg)
                except websockets.exceptions.ConnectionClosed:
                    LOGGER.warning("WebSocket connection closed by upstream server.")
                    pass

            task_c2s = asyncio.create_task(client_to_server())
            task_s2c = asyncio.create_task(server_to_client())

            done, pending = await asyncio.wait(
                [task_c2s, task_s2c],
                return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()

    except WebSocketDisconnect:
        pass
    except Exception as e:
        LOGGER.error(f"WebSocket Error: {e}")
        if websocket.client_state.name != "DISCONNECTED":
            await websocket.close(code=1011, reason="Internal Server Error")


class ChatCompletionRequest(BaseModel):
    """OpenAI-compatible chat completion request body."""
    model: str = Field(..., description="Model ID to route to")

    class Config:
        extra = 'allow'


@app.post('/v1/chat/completions', tags=["OpenAI Standard"])
async def proxy_chat_completions(
    body: ChatCompletionRequest,
    request: Request,
    token=Depends(bearer_scheme),
):
    """Proxy the chat completion request to the correct server."""
    body_bytes = await request.body()
    model_name = body.model

    forward_url, auth_data = await resolve_forward_url(token)

    if forward_url:
        target_url = f"{forward_url.rstrip('/')}/chat/completions"
    else:
        db_model = await get_model(model_name)
        if not db_model:
            raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found.")
        target_url = f"{db_model['url']}/chat/completions"

    response = await forward_request(request, target_url, body_bytes, model_name)

    if response.status_code == 499:
        LOGGER.warning(f"Client disconnected before upstream response for chat model={model_name}")
        return response

    # Build log info for Kafka/file logging
    user_api_key = ''
    agent_api_key = ''
    if auth_data:
        if auth_data.get('type') == 'agent':
            agent_api_key = auth_data.get('agent_api_key', '')
            user_api_key = auth_data.get('user_api_key', '')
        else:
            user_api_key = auth_data.get('api_key', '')

    # Only log fully-successful (2xx streaming) responses
    if 200 <= response.status_code < 300 and isinstance(response, StreamingResponse):
        original_iterator = response.body_iterator

        async def logged_stream():
            chunks: list[bytes] = []
            try:
                async for chunk in original_iterator:
                    chunks.append(chunk)
                    yield chunk
                try:
                    request_obj = json.loads(body_bytes)
                except json.JSONDecodeError:
                    request_obj = body_bytes.decode('utf-8', errors='replace')

                response_text = b''.join(chunks).decode('utf-8', errors='replace')
                response_obj = _parse_response(response_text)

                log_entry = {
                    "user_api_key": user_api_key,
                    "agent_api_key": agent_api_key,
                    "request": request_obj,
                    "response": response_obj,
                }
                await _send_kafka_log(log_entry)
            except (asyncio.CancelledError, GeneratorExit):
                LOGGER.warning(f"Client disconnected during chat stream for model={model_name}")
                raise
            except Exception:
                LOGGER.warning(f"Chat stream interrupted for model={model_name}")
                raise

        response.body_iterator = logged_stream()

    return response


# ==========================================
# Authentication Endpoints
# ==========================================

@app.post('/apikey', tags=["Authentication"])
async def create_api_key(
    body: dict = Body(..., description="Request body forwarded to auth_url"),
):
    """Create a new API key. The request body is forwarded to auth_url.
    If auth_url returns 200, the entire auth_response (sorted keys) is hashed and stored."""
    if config.authless_mode:
        raise HTTPException(status_code=404)
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                config.auth_url,
                json=body,
                timeout=30.0,
            )
        except Exception as e:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to reach auth_url: {str(e)}",
            )

    if resp.status_code != 200:
        try:
            error_detail = resp.json()
        except Exception:
            error_detail = resp.text
        return JSONResponse(
            status_code=resp.status_code,
            content={
                "error": {
                    "message": f"auth_url returned {resp.status_code}",
                    "type": "auth_error",
                    "param": None,
                    "code": "auth_url_error",
                    "detail": error_detail,
                }
            },
        )

    auth_response = resp.json()
    api_key = await generate_account_auth(auth_response)
    return {"api_key": api_key}


@app.get('/apikey', tags=["Authentication"])
async def get_api_key_data(
    token: Optional[HTTPAuthorizationCredentials] = Depends(auth_api_key),
):
    """Retrieve the data associated with your API key."""
    if config.authless_mode:
        raise HTTPException(status_code=404)
    err = authenticated_auth(token)
    if err:
        return err

    data = await check_api_key(token.credentials)
    if data is None:
        return JSONResponse(
            status_code=401,
            content={
                "error": {
                    "message": "Invalid API key. No data found.",
                    "type": "invalid_request_error",
                    "param": None,
                    "code": "invalid_api_key",
                }
            },
        )

    if data.get('type') == 'agent':
        user_data = await check_api_key(data.get('user_api_key', ''))
        data.pop('user_api_key', None)
        if user_data:
            user_data.pop('api_key', None)
        data['user_data'] = user_data

    return data


# ==========================================
# Agent Management Endpoints
# ==========================================

@app.post('/agent', tags=["Agent Management"])
async def create_agent(
    agent_name: str = Body(..., description="Name of the agent"),
    agent_description: Optional[str] = Body(None, description="Optional description"),
    token: Optional[HTTPAuthorizationCredentials] = Depends(auth_api_key),
):
    """Generate a new agent API key. Requires user authentication."""
    if config.authless_mode:
        raise HTTPException(status_code=404)
    err = authenticated_auth(token)
    if err:
        return err

    auth_data = await check_api_key(token.credentials)
    if auth_data is None:
        return JSONResponse(
            status_code=401,
            content={
                "error": {
                    "message": "Invalid API key.",
                    "type": "invalid_request_error",
                    "param": None,
                    "code": "invalid_api_key",
                }
            },
        )

    user_api_key = auth_data.get('api_key', token.credentials)
    api_key = await generate_agent_auth(agent_name, agent_description, user_api_key)
    return {"agent_api_key": api_key}


@app.delete('/agent', tags=["Agent Management"])
async def delete_agent(
    agent_api_key: str = Body(..., description="API key of the agent to delete", embed=True),
    token: Optional[HTTPAuthorizationCredentials] = Depends(auth_api_key),
):
    """Delete an agent API key. Only the user who created it can delete it."""
    if config.authless_mode:
        raise HTTPException(status_code=404)
    err = authenticated_auth(token)
    if err:
        return err

    auth_data = await check_api_key(token.credentials)
    if auth_data is None:
        return JSONResponse(
            status_code=401,
            content={
                "error": {
                    "message": "Invalid API key.",
                    "type": "invalid_request_error",
                    "param": None,
                    "code": "invalid_api_key",
                }
            },
        )

    user_api_key = auth_data.get('api_key', token.credentials)
    result = await delete_agent_auth(agent_api_key, user_api_key)

    if result is None:
        return JSONResponse(
            status_code=404,
            content={
                "error": {
                    "message": "Agent not found or you do not have permission to delete it.",
                    "type": "agent_not_found",
                    "param": None,
                    "code": "agent_not_found",
                }
            },
        )

    return {"status": "success", "deleted": result}


# ==========================================
# Forward Route Management Endpoints
# ==========================================

class ForwardRequest(BaseModel):
    url: str = Field(..., description="Target URL to forward all requests to")


@app.post('/forward', tags=["Forward Management"])
async def create_forward(
    payload: ForwardRequest,
    token: Optional[HTTPAuthorizationCredentials] = Depends(auth_api_key),
):
    """Set a forward URL for your API key. All subsequent requests will be routed to this URL."""
    auth_data, err = await require_auth(token)
    if err:
        return err

    user_api_key = auth_data.get('api_key', token.credentials) if auth_data else (token.credentials if token and token.credentials else '')
    await set_forward_url(user_api_key, payload.url)
    return {"status": "success", "message": f"Forward URL set to {payload.url}"}


@app.delete('/forward', tags=["Forward Management"])
async def delete_forward(
    token: Optional[HTTPAuthorizationCredentials] = Depends(auth_api_key),
):
    """Remove the forward URL for your API key."""
    auth_data, err = await require_auth(token)
    if err:
        return err

    user_api_key = auth_data.get('api_key', token.credentials) if auth_data else (token.credentials if token and token.credentials else '')
    deleted = await delete_forward_url(user_api_key)

    if not deleted:
        return JSONResponse(
            status_code=404,
            content={
                "error": {
                    "message": "No forward URL found for this API key.",
                    "type": "not_found",
                    "param": None,
                    "code": "forward_not_found",
                }
            },
        )

    return {"status": "success", "message": "Forward URL removed."}


# ==========================================
# Catch-all Proxy (must be last)
# ==========================================

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"], include_in_schema=False)
async def catch_all_proxy(
    request: Request,
    path: str,
    token=Depends(bearer_scheme),
):
    """Catch-all route that dynamically forwards requests based on payload 'model'."""
    target_model = None
    body_bytes = b""

    if request.method in ["POST", "PUT", "PATCH"]:
        body_bytes = await request.body()
        try:
            body_json = json.loads(body_bytes)
            target_model = body_json.get("model")
        except json.JSONDecodeError:
            pass

    forward_url, _ = await resolve_forward_url(token)

    if forward_url:
        target_url = f"{forward_url.rstrip('/')}/{path}"
        return await forward_request(request, target_url, body_bytes, target_model or path)

    if not target_model:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot route generic path '/{path}' because no 'model' was specified in the payload."
        )

    db_model = await get_model(target_model)
    if not db_model:
        raise HTTPException(status_code=404, detail=f"Model '{target_model}' not found.")

    base_url = db_model['url'].rstrip('/').removesuffix('/v1')
    target_url = f"{base_url}/{path}"

    return await forward_request(request, target_url, body_bytes, target_model)
