# app/main.py
import asyncio
import json
import logging
import timeit

import httpx
import websockets
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import RedirectResponse, Response, StreamingResponse, JSONResponse
from fastapi.security import HTTPBearer
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from typing import List

from .database import Base, engine, get_db, VllmModel, VoiceModel

LOGGER_ACCESS = logging.getLogger('gunicorn.access')
LOGGER = logging.getLogger('uvicorn.error')

# Global HTTP client for connection pooling
http_client: httpx.AsyncClient = None

# Bearer token security scheme (non-enforcing — shows in Swagger but doesn't block requests without a token)
bearer_scheme = HTTPBearer(auto_error=False)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    # Initialize DB tables
    Base.metadata.create_all(bind=engine)
    # Initialize global async HTTP client
    http_client = httpx.AsyncClient(timeout=httpx.Timeout(None))
    yield
    # Cleanup on shutdown
    await http_client.aclose()

app = FastAPI(
    title='vLLM Forwarder API',
    description='API for managing and proxying requests to multiple vLLM servers with OpenAI-compatible endpoints.',
    version='1.0.0',
    lifespan=lifespan,
    dependencies=[Depends(bearer_scheme)]
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
    return RedirectResponse(app.root_path+'/docs')


# ==========================================
# Core Forwarding Engine
# ==========================================

async def forward_request(
    request: Request,
    target_url: str,
    body_bytes: bytes,
    model_name: str
) -> Response:
    """
    Core engine for forwarding requests to vLLM.
    Handles headers, disconnect racing, error catching, and streaming.
    """
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
                "message": f"The upstream vLLM server hosting '{model_name}' is currently unreachable.",
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
            error_data = {"error": {"message": upstream_response.text, "type": "vllm_error", "code": upstream_response.status_code}}
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
    url: str # e.g., "http://192.168.1.50:8000/v1"

@app.post('/model', tags=["Management"])
async def add_model(payload: AddModelRequest, db: Session = Depends(get_db)):
    """Fetch models from a vLLM server and save them to the DB."""
    base_url = payload.url.rstrip('/')
    target_url = f"{base_url}/models"

    try:
        response = await http_client.get(target_url, timeout=10.0)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to connect to {target_url}. Error: {str(e)}")

    models_list = data.get("data", [])
    if not models_list:
        raise HTTPException(status_code=400, detail="No models found at the target URL.")

    added_models = []
    for model_data in models_list:
        model_id = model_data.get("id")
        if not model_id:
            continue

        existing_model = db.query(VllmModel).filter(VllmModel.id == model_id).first()
        if existing_model:
            existing_model.url = base_url
            existing_model.properties = model_data
        else:
            new_model = VllmModel(id=model_id, url=base_url, properties=model_data)
            db.add(new_model)

        added_models.append(model_id)

    db.commit()
    return {"status": "success", "added_models": added_models}

@app.get('/v1/models', tags=["OpenAI Standard"])
async def list_models(db: Session = Depends(get_db)):
    """List all models currently in the proxy database."""
    models = db.query(VllmModel).all()
    # Reconstruct the OpenAI standard response
    return {
        "object": "list",
        "data": [m.properties for m in models]
    }

@app.delete('/model/{model_name:path}', tags=["Management"])
async def delete_model(model_name: str, db: Session = Depends(get_db)):
    """Delete a model route from the database."""
    existing_model = db.query(VllmModel).filter(VllmModel.id == model_name).first()
    if not existing_model:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found.")
    
    db.delete(existing_model)
    db.commit()
    return {"status": "success", "message": f"Deleted model '{model_name}'"}


# ==========================================
# Proxy & Management Endpoints (Speech API)
# ==========================================

class AddVoiceRequest(BaseModel):
    url: str
    extra_kwargs: List[str] = Field(default_factory=list)

@app.post('/voice', tags=["Speech Management"])
async def add_voice(payload: AddVoiceRequest, db: Session = Depends(get_db)):
    """Fetch voices from a vLLM server and save them to the DB."""
    base_url = payload.url.rstrip('/')
    target_url = f"{base_url}/audio/voices"
    try:
        response = await http_client.get(target_url, timeout=10.0)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to connect to {target_url}. Error: {str(e)}")

    voices_list = data.get("voices", [])
    if not voices_list:
        raise HTTPException(status_code=400, detail="No voices found at the target URL.")

    added_voices = []
    for voice_id in voices_list:
        existing_voice = db.query(VoiceModel).filter(VoiceModel.id == voice_id).first()
        if existing_voice:
            existing_voice.url = base_url
            existing_voice.extra_kwargs = payload.extra_kwargs
        else:
            new_voice = VoiceModel(id=voice_id, url=base_url, extra_kwargs=payload.extra_kwargs)
            db.add(new_voice)
        added_voices.append(voice_id)

    db.commit()
    return {"status": "success", "added_voices": added_voices}

@app.get('/v1/audio/voices', tags=["Speech Standard"])
async def list_voices(db: Session = Depends(get_db)):
    """List all voices currently registered in the database."""
    voices = db.query(VoiceModel).all()
    return {"voices": [v.id for v in voices], "uploaded_voices": []}

@app.delete('/voice/url/{url:path}', tags=["Speech Management"])
async def delete_voice_by_url(url: str, db: Session = Depends(get_db)):
    """Delete list of voices by their URL."""
    existing_voices = db.query(VoiceModel).filter(VoiceModel.url == url).all()
    if not existing_voices:
        raise HTTPException(status_code=404, detail=f"Voices with URL '{url}' not found.")
    for voice in existing_voices:
        db.delete(voice)
    db.commit()
    return {"status": "success", "message": f"Deleted voices with URL '{url}'"}

@app.delete('/voice/{voice_name:path}', tags=["Speech Management"])
async def delete_voice(voice_name: str, db: Session = Depends(get_db)):
    """Delete a voice route from the database."""
    existing_voice = db.query(VoiceModel).filter(VoiceModel.id == voice_name).first()
    if not existing_voice:
        raise HTTPException(status_code=404, detail=f"Voice '{voice_name}' not found.")
    db.delete(existing_voice)
    db.commit()
    return {"status": "success", "message": f"Deleted voice '{voice_name}'"}

class SpeechRequest(BaseModel):
    """OpenAI-compatible speech (TTS) request body. Only `voice` is required for routing."""
    voice: str = Field(..., description="Voice ID for routing")

    class Config:
        extra = 'allow'

@app.post('/v1/audio/speech', tags=["Speech Standard"])
async def proxy_audio_speech(body: SpeechRequest, request: Request, db: Session = Depends(get_db)):
    """Proxy the REST Speech endpoint to the correct server based on Voice."""
    body_bytes = await request.body()
    voice_name = body.voice

    db_voice = db.query(VoiceModel).filter(VoiceModel.id == voice_name).first()
    if not db_voice:
        raise HTTPException(status_code=404, detail=f"Voice '{voice_name}' not found in routing database.")

    target_url = f"{db_voice.url}/audio/speech"
    return await forward_request(request, target_url, body_bytes, voice_name)


# ==========================================
# Speech WebSocket Endpoint
# ==========================================

@app.websocket("/v1/audio/speech/stream")
async def websocket_speech_stream(websocket: WebSocket, db: Session = Depends(get_db)):
    await websocket.accept()
    try:
        # Wait for the first client message, which MUST be session.config
        first_msg_str = await websocket.receive_text()
        first_msg = json.loads(first_msg_str)
        
        if first_msg.get("type") != "session.config":
            await websocket.close(code=1008, reason="First message must be session.config")
            return
        
        # Identify routing by Voice
        voice_name = first_msg.get("voice", "vivian")
        db_voice = db.query(VoiceModel).filter(VoiceModel.id == voice_name).first()
        if not db_voice:
            await websocket.close(code=1008, reason=f"Voice '{voice_name}' not found")
            return

        # extra_kwargs processing engine
        allowed_kwargs = db_voice.extra_kwargs or []
        user_extra_kwargs = first_msg.pop("extra_kwargs", {}) # Pop removes it cleanly from first_msg

        # Lift valid items from the dictionary into the top level of session.config
        if isinstance(user_extra_kwargs, dict):
            for key, val in user_extra_kwargs.items():
                if key in allowed_kwargs:
                    first_msg[key] = val

        # Calculate Upstream WebSocket URI
        base_ws_url = db_voice.url.replace("http://", "ws://").replace("https://", "wss://")
        target_ws_url = f"{base_ws_url}/audio/speech/stream"

        # Open Upstream Connection
        async with websockets.connect(
            target_ws_url, 
            max_size=None, 
            ping_interval=None
        ) as upstream_ws:
            # Relay the modified session.config
            await upstream_ws.send(json.dumps(first_msg))

            # Background Task 1: Forward Client Text -> Upstream Server
            async def client_to_server():
                try:
                    while True:
                        msg = await websocket.receive_text() # Client strictly sends JSON text
                        await upstream_ws.send(msg)
                except WebSocketDisconnect:
                    LOGGER.warning("WebSocket disconnected by client.")
                    pass

            # Background Task 2: Forward Upstream Server -> Client
            async def server_to_client():
                try:
                    while True:
                        msg = await upstream_ws.recv()
                        # The speech API mixes strings (JSON metadata) and bytes (PCM streams)
                        if isinstance(msg, str):
                            await websocket.send_text(msg)
                        else:
                            await websocket.send_bytes(msg)
                except websockets.exceptions.ConnectionClosed:
                    LOGGER.warning("WebSocket connection closed by upstream server.")
                    pass

            # Run both streaming tasks simultaneously. When one drops, cancel the other.
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
    """OpenAI-compatible chat completion request body. Only `model` is required for routing."""
    model: str = Field(..., description="Model ID to route to")

    class Config:
        extra = 'allow'  # Forward-compatible with newer OpenAI parameters

@app.post('/v1/chat/completions', tags=["OpenAI Standard"])
async def proxy_chat_completions(body: ChatCompletionRequest, request: Request, db: Session = Depends(get_db)):
    """Proxy the chat completion request to the correct vLLM server."""
    body_bytes = await request.body()
    model_name = body.model

    db_model = db.query(VllmModel).filter(VllmModel.id == model_name).first()
    if not db_model:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found in routing database.")

    target_url = f"{db_model.url}/chat/completions"

    # Hand off to the core engine
    return await forward_request(request, target_url, body_bytes, model_name)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"], include_in_schema=False)
async def catch_all_proxy(request: Request, path: str, db: Session = Depends(get_db)):
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

    if not target_model:
        raise HTTPException(
            status_code=400, 
            detail=f"Cannot route generic path '/{path}' because no 'model' was specified in the payload."
        )
        
    db_model = db.query(VllmModel).filter(VllmModel.id == target_model).first()
    if not db_model:
        raise HTTPException(status_code=404, detail=f"Model '{target_model}' not found in routing database.")
    
    base_url = db_model.url.rstrip('/').removesuffix('/v1')
    target_url = f"{base_url}/{path}"

    # Hand off to the core engine
    return await forward_request(request, target_url, body_bytes, target_model)