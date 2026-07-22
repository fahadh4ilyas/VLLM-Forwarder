import json
import hashlib
from base64 import urlsafe_b64encode

from fastapi import Depends
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from .config import config
from .database import (
    SessionLocal, uses_mongo, get_mongo_db,
    UserAuth, AgentAuth, ForwardRoute, VllmModel, VoiceModel,
)
from .tools import get_redis_connection_pool, get_redis_client

# Redis connection pool — initialised once at module import if enabled
_redis_pool = get_redis_connection_pool() if config.use_redis else None

_CACHE_TTL = 24 * 60 * 60  # 24 hours


def _get_redis():
    if _redis_pool is None:
        return None
    return get_redis_client(_redis_pool)


# ==========================================
# FastAPI Bearer dependency
# ==========================================

bearer_scheme = HTTPBearer(auto_error=False)

async def auth_api_key(
    token: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> HTTPAuthorizationCredentials:
    if token is None:
        return HTTPAuthorizationCredentials(scheme='Empty', credentials='')
    return token


# ==========================================
# API Key Validation
# ==========================================

async def check_api_key(key: str) -> dict | None:
    """Validate an API key (user or agent) and return its data, or None if invalid."""
    if not key:
        return None

    cache_key = f'apikey_{config.name_prefix}_{key}'
    redis = _get_redis()
    if redis is not None:
        cached = await redis.get(cache_key)
        if cached is not None:
            return json.loads(cached) if cached != '__NONE__' else None

    result = None

    # --- User lookup ---
    if uses_mongo():
        mongo_db = get_mongo_db()
        doc = await mongo_db.user_auth.find_one({
            'api_key': key,
            'prefix': config.name_prefix,
        })
        if doc:
            doc.pop('_id', None)
            doc['type'] = 'user'
            result = doc
    else:
        db = SessionLocal()
        try:
            auth = (
                db.query(UserAuth)
                .filter(UserAuth.api_key == key, UserAuth.prefix == config.name_prefix)
                .first()
            )
            if auth:
                result = {
                    'api_key': auth.api_key,
                    'response_data': auth.response_data,
                    'type': 'user',
                }
        finally:
            db.close()

    # --- Agent lookup (only if user not found) ---
    if result is None:
        if uses_mongo():
            mongo_db = get_mongo_db()
            doc = await mongo_db.agent_auth.find_one({
                'agent_api_key': key,
                'prefix': config.name_prefix,
            })
            if doc:
                doc.pop('_id', None)
                doc['type'] = 'agent'
                result = doc
        else:
            db = SessionLocal()
            try:
                agent = (
                    db.query(AgentAuth)
                    .filter(AgentAuth.agent_api_key == key, AgentAuth.prefix == config.name_prefix)
                    .first()
                )
                if agent:
                    result = {
                        'agent_api_key': agent.agent_api_key,
                        'agent_name': agent.agent_name,
                        'agent_description': agent.agent_description,
                        'user_api_key': agent.user_api_key,
                        'type': 'agent',
                    }
            finally:
                db.close()

    if redis is not None:
        await redis.set(cache_key, json.dumps(result) if result else '__NONE__', ex=_CACHE_TTL if result else 60)

    return result


def authenticated_auth(auth_token: HTTPAuthorizationCredentials) -> JSONResponse | None:
    """Check Bearer token presence. Returns error JSONResponse or None if OK."""
    if auth_token.scheme == 'Empty':
        return JSONResponse(
            status_code=401,
            content={
                "error": {
                    "message": "You didn't provide an API key. Use Authorization: Bearer YOUR_KEY.",
                    "type": "invalid_request_error",
                    "param": None,
                    "code": None,
                }
            },
        )
    return None


async def require_auth(
    token: HTTPAuthorizationCredentials,
) -> tuple[dict | None, JSONResponse | None]:
    """Validate any API key (user or agent). Returns (auth_data, error).

    In authless mode, skips validation entirely.
    """
    if config.authless_mode:
        return None, None
    err = authenticated_auth(token)
    if err:
        return None, err
    auth_data = await check_api_key(token.credentials)
    if auth_data is None:
        return None, JSONResponse(status_code=401, content={
            "error": {"message": "Invalid API key.", "type": "invalid_request_error", "param": None, "code": "invalid_api_key"}
        })
    return auth_data, None


async def require_user_auth(
    token: HTTPAuthorizationCredentials,
) -> tuple[dict | None, JSONResponse | None]:
    """Validate a user API key only. Returns (auth_data, error).

    In authless mode, skips validation entirely.
    Otherwise: missing token → 401, invalid key → 401, agent key → 403.
    """
    auth_data, err = await require_auth(token)
    if err or auth_data is None:
        return auth_data, err
    if auth_data.get('type') != 'user':
        return None, JSONResponse(status_code=403, content={
            "error": {"message": "Only user API keys can perform this action.", "type": "invalid_request_error", "param": None, "code": "access_denied"}
        })
    return auth_data, None


# ==========================================
# User API Key (POST /apikey)
# ==========================================

async def generate_account_auth(auth_response: dict) -> str:
    """Hash the auth_response (sorted keys) with secret_key, save to DB, return API key.

    The key is deterministic — same auth_response always yields the same key.
    """
    data_str = json.dumps(auth_response, sort_keys=True) + ':' + config.secret_key + ':'
    api_key = 'pr-' + urlsafe_b64encode(
        hashlib.sha256(data_str.encode()).digest()
    ).decode()[:-1]

    if uses_mongo():
        mongo_db = get_mongo_db()
        existing = await mongo_db.user_auth.find_one({
            'api_key': api_key,
            'prefix': config.name_prefix,
        })
        if existing:
            api_key = existing['api_key']

        else:
            await mongo_db.user_auth.insert_one({
                'api_key': api_key,
                'response_data': auth_response,
                'prefix': config.name_prefix,
            })
    else:
        db = SessionLocal()
        try:
            existing = (
                db.query(UserAuth)
                .filter(UserAuth.api_key == api_key, UserAuth.prefix == config.name_prefix)
                .first()
            )
            if existing:
                api_key = existing.api_key
            else:
                user_auth = UserAuth(
                    api_key=api_key,
                    response_data=auth_response,
                    prefix=config.name_prefix,
                )
                db.add(user_auth)
                db.commit()
        finally:
            db.close()

    redis = _get_redis()
    if redis is not None:
        await redis.set(f'apikey_{config.name_prefix}_{api_key}', json.dumps({'api_key': api_key, 'response_data': auth_response, 'type': 'user'}), ex=_CACHE_TTL)

    return api_key


# ==========================================
# Agent API Key (POST/DELETE /agent)
# ==========================================

async def generate_agent_auth(
    agent_name: str, agent_description: str | None, user_api_key: str
) -> str:
    """Create agent API key, save to DB, return the key."""
    api_key = 'ag-' + urlsafe_b64encode(
        hashlib.sha256((agent_name + user_api_key).encode()).digest()
    ).decode()[:-1]

    prefix = config.name_prefix

    if uses_mongo():
        mongo_db = get_mongo_db()
        existing = await mongo_db.agent_auth.find_one({
            'agent_name': agent_name,
            'user_api_key': user_api_key,
            'prefix': prefix,
        })
        if existing:
            api_key = existing['agent_api_key']
        else:
            await mongo_db.agent_auth.insert_one({
                'agent_api_key': api_key,
            'agent_name': agent_name,
            'agent_description': agent_description,
            'user_api_key': user_api_key,
            'prefix': prefix,
        })
    else:
        db = SessionLocal()
        try:
            existing = (
                db.query(AgentAuth)
                .filter(
                    AgentAuth.agent_name == agent_name,
                    AgentAuth.user_api_key == user_api_key,
                    AgentAuth.prefix == prefix,
                )
                .first()
            )
            if existing:
                api_key = existing.agent_api_key
            else:
                agent = AgentAuth(
                    agent_api_key=api_key,
                    agent_name=agent_name,
                    agent_description=agent_description,
                    user_api_key=user_api_key,
                    prefix=prefix,
                )
                db.add(agent)
                db.commit()
        finally:
            db.close()

    redis = _get_redis()
    if redis is not None:
        await redis.set(f'apikey_{config.name_prefix}_{api_key}', json.dumps({
            'agent_api_key': api_key, 'agent_name': agent_name,
            'agent_description': agent_description, 'user_api_key': user_api_key,
            'type': 'agent',
        }), ex=_CACHE_TTL)

    return api_key


async def delete_agent_auth(agent_api_key: str, user_api_key: str) -> dict | None:
    """Delete an agent by its API key, only if owned by the user."""
    prefix = config.name_prefix

    if uses_mongo():
        mongo_db = get_mongo_db()
        result = await mongo_db.agent_auth.find_one_and_delete({
            'agent_api_key': agent_api_key,
            'user_api_key': user_api_key,
            'prefix': prefix,
        })
        if result:
            result.pop('_id', None)
            redis = _get_redis()
            if redis is not None:
                await redis.delete(f'apikey_{config.name_prefix}_{agent_api_key}')
            return result
        return None
    else:
        db = SessionLocal()
        try:
            agent = (
                db.query(AgentAuth)
                .filter(
                    AgentAuth.agent_api_key == agent_api_key,
                    AgentAuth.user_api_key == user_api_key,
                    AgentAuth.prefix == prefix,
                )
                .first()
            )
            if agent:
                result = {
                    'agent_api_key': agent.agent_api_key,
                    'agent_name': agent.agent_name,
                    'agent_description': agent.agent_description,
                    'user_api_key': agent.user_api_key,
                }
                db.delete(agent)
                db.commit()
                redis = _get_redis()
                if redis is not None:
                    await redis.delete(f'apikey_{config.name_prefix}_{agent_api_key}')
                return result
            return None
        finally:
            db.close()


# ==========================================
# Forward Route (POST/DELETE /forward)
# ==========================================

async def get_forward_url(user_api_key: str) -> str | None:
    """Get the forward URL for a user, if set."""
    prefix = config.name_prefix
    cache_key = f'forward_{prefix}_{user_api_key}'

    redis = _get_redis()
    if redis is not None:
        cached = await redis.get(cache_key)
        if cached is not None:
            return cached if cached != '__NONE__' else None

    result = None
    if uses_mongo():
        mongo_db = get_mongo_db()
        doc = await mongo_db.forward_routes.find_one({
            'user_api_key': user_api_key,
            'prefix': prefix,
        })
        result = doc['url'] if doc else None
    else:
        db = SessionLocal()
        try:
            route = (
                db.query(ForwardRoute)
                .filter(
                    ForwardRoute.user_api_key == user_api_key,
                    ForwardRoute.prefix == prefix,
                )
                .first()
            )
            result = route.url if route else None
        finally:
            db.close()

    if redis is not None:
        await redis.set(cache_key, result if result else '__NONE__', ex=_CACHE_TTL)

    return result


async def set_forward_url(user_api_key: str, url: str) -> None:
    """Set or update the forward URL for a user."""
    prefix = config.name_prefix

    if uses_mongo():
        mongo_db = get_mongo_db()
        await mongo_db.forward_routes.replace_one(
            {'user_api_key': user_api_key, 'prefix': prefix},
            {'user_api_key': user_api_key, 'url': url, 'prefix': prefix},
            upsert=True,
        )
    else:
        db = SessionLocal()
        try:
            route = (
                db.query(ForwardRoute)
                .filter(
                    ForwardRoute.user_api_key == user_api_key,
                    ForwardRoute.prefix == prefix,
                )
                .first()
            )
            if route:
                route.url = url
            else:
                route = ForwardRoute(user_api_key=user_api_key, url=url, prefix=prefix)
                db.add(route)
            db.commit()
        finally:
            db.close()

    redis = _get_redis()
    if redis is not None:
        await redis.set(f'forward_{prefix}_{user_api_key}', url, ex=_CACHE_TTL)


async def delete_forward_url(user_api_key: str) -> bool:
    """Delete the forward URL for a user. Returns True if something was deleted."""
    prefix = config.name_prefix
    deleted = False

    if uses_mongo():
        mongo_db = get_mongo_db()
        result = await mongo_db.forward_routes.delete_one({
            'user_api_key': user_api_key,
            'prefix': prefix,
        })
        deleted = result.deleted_count > 0
    else:
        db = SessionLocal()
        try:
            route = (
                db.query(ForwardRoute)
                .filter(
                    ForwardRoute.user_api_key == user_api_key,
                    ForwardRoute.prefix == prefix,
                )
                .first()
            )
            if route:
                db.delete(route)
                db.commit()
                deleted = True
        finally:
            db.close()

    if deleted:
        redis = _get_redis()
        if redis is not None:
            await redis.delete(f'forward_{prefix}_{user_api_key}')

    return deleted


async def resolve_forward_url(
    token: HTTPAuthorizationCredentials,
) -> str | None:
    """Resolve forward URL from any API key. Returns forward_url or None."""
    if not (token and token.credentials):
        return None
    return await get_forward_url(token.credentials)


# ==========================================
# Model CRUD (dual backend)
# ==========================================

async def get_model(model_name: str) -> dict | None:
    """Get a model by name, scoped to current prefix."""
    prefix = config.name_prefix
    cache_key = f'model_{prefix}_{model_name}'

    redis = _get_redis()
    if redis is not None:
        cached = await redis.get(cache_key)
        if cached is not None:
            return json.loads(cached) if cached != '__NONE__' else None

    result = None
    if uses_mongo():
        mongo_db = get_mongo_db()
        doc = await mongo_db.vllm_models.find_one({'id': model_name, 'prefix': prefix})
        if doc:
            doc.pop('_id', None)
            result = doc
    else:
        db = SessionLocal()
        try:
            model = (
                db.query(VllmModel)
                .filter(VllmModel.id == model_name, VllmModel.prefix == prefix)
                .first()
            )
            if model:
                result = {'id': model.id, 'url': model.url, 'properties': model.properties}
        finally:
            db.close()

    if redis is not None:
        await redis.set(cache_key, json.dumps(result) if result else '__NONE__', ex=_CACHE_TTL)

    return result


async def list_models() -> list[dict]:
    """List all models for current prefix."""
    prefix = config.name_prefix
    cache_key = f'models_{prefix}'

    redis = _get_redis()
    if redis is not None:
        cached = await redis.get(cache_key)
        if cached is not None:
            return json.loads(cached)

    if uses_mongo():
        mongo_db = get_mongo_db()
        cursor = mongo_db.vllm_models.find({'prefix': prefix})
        result = [doc async for doc in cursor]
    else:
        db = SessionLocal()
        try:
            models = db.query(VllmModel).filter(VllmModel.prefix == prefix).all()
            result = [{'id': m.id, 'url': m.url, 'properties': m.properties} for m in models]
        finally:
            db.close()

    if redis is not None:
        await redis.set(cache_key, json.dumps(result), ex=_CACHE_TTL)

    return result


async def upsert_models(models_data: list[dict], base_url: str) -> list[str]:
    """Insert or update models from a vLLM /v1/models response. Returns list of model IDs added."""
    prefix = config.name_prefix
    added = []

    if uses_mongo():
        mongo_db = get_mongo_db()
        for md in models_data:
            model_id = md.get('id')
            if not model_id:
                continue
            await mongo_db.vllm_models.replace_one(
                {'id': model_id, 'prefix': prefix},
                {'id': model_id, 'url': base_url, 'properties': md, 'prefix': prefix},
                upsert=True,
            )
            added.append(model_id)
    else:
        db = SessionLocal()
        try:
            for md in models_data:
                model_id = md.get('id')
                if not model_id:
                    continue
                existing = (
                    db.query(VllmModel)
                    .filter(VllmModel.id == model_id, VllmModel.prefix == prefix)
                    .first()
                )
                if existing:
                    existing.url = base_url
                    existing.properties = md
                else:
                    db.add(VllmModel(id=model_id, url=base_url, properties=md, prefix=prefix))
                added.append(model_id)
            db.commit()
        finally:
            db.close()

    redis = _get_redis()
    if redis is not None:
        await redis.delete(f'models_{prefix}')

    return added


async def delete_model_by_name(model_name: str) -> bool:
    """Delete a model by name, scoped to current prefix. Returns True if deleted."""
    prefix = config.name_prefix
    deleted = False

    if uses_mongo():
        mongo_db = get_mongo_db()
        result = await mongo_db.vllm_models.delete_one({'id': model_name, 'prefix': prefix})
        deleted = result.deleted_count > 0
    else:
        db = SessionLocal()
        try:
            model = (
                db.query(VllmModel)
                .filter(VllmModel.id == model_name, VllmModel.prefix == prefix)
                .first()
            )
            if model:
                db.delete(model)
                db.commit()
                deleted = True
        finally:
            db.close()

    if deleted:
        redis = _get_redis()
        if redis is not None:
            await redis.delete(f'model_{prefix}_{model_name}', f'models_{prefix}')

    return deleted


# ==========================================
# Voice CRUD (dual backend)
# ==========================================

async def get_voice(voice_name: str) -> dict | None:
    """Get a voice by name, scoped to current prefix."""
    prefix = config.name_prefix
    cache_key = f'voice_{prefix}_{voice_name}'

    redis = _get_redis()
    if redis is not None:
        cached = await redis.get(cache_key)
        if cached is not None:
            return json.loads(cached) if cached != '__NONE__' else None

    result = None
    if uses_mongo():
        mongo_db = get_mongo_db()
        doc = await mongo_db.speech_voices.find_one({'id': voice_name, 'prefix': prefix})
        if doc:
            doc.pop('_id', None)
            result = doc
    else:
        db = SessionLocal()
        try:
            voice = (
                db.query(VoiceModel)
                .filter(VoiceModel.id == voice_name, VoiceModel.prefix == prefix)
                .first()
            )
            if voice:
                result = {
                    'id': voice.id, 'url': voice.url,
                    'extra_kwargs': voice.extra_kwargs,
                }
        finally:
            db.close()

    if redis is not None:
        await redis.set(cache_key, json.dumps(result) if result else '__NONE__', ex=_CACHE_TTL)

    return result


async def list_voices_all() -> list[dict]:
    """List all voices for current prefix."""
    prefix = config.name_prefix
    cache_key = f'voices_{prefix}'

    redis = _get_redis()
    if redis is not None:
        cached = await redis.get(cache_key)
        if cached is not None:
            return json.loads(cached)

    if uses_mongo():
        mongo_db = get_mongo_db()
        cursor = mongo_db.speech_voices.find({'prefix': prefix})
        result = [doc async for doc in cursor]
    else:
        db = SessionLocal()
        try:
            voices = db.query(VoiceModel).filter(VoiceModel.prefix == prefix).all()
            result = [
                {'id': v.id, 'url': v.url, 'extra_kwargs': v.extra_kwargs}
                for v in voices
            ]
        finally:
            db.close()

    if redis is not None:
        await redis.set(cache_key, json.dumps(result), ex=_CACHE_TTL)

    return result


async def upsert_voices(voices_list: list[str], base_url: str, extra_kwargs: list[str]) -> list[str]:
    """Insert or update voices. Returns list of voice IDs added."""
    prefix = config.name_prefix
    added = []

    if uses_mongo():
        mongo_db = get_mongo_db()
        for voice_id in voices_list:
            await mongo_db.speech_voices.replace_one(
                {'id': voice_id, 'prefix': prefix},
                {'id': voice_id, 'url': base_url, 'extra_kwargs': extra_kwargs, 'prefix': prefix},
                upsert=True,
            )
            added.append(voice_id)
    else:
        db = SessionLocal()
        try:
            for voice_id in voices_list:
                existing = (
                    db.query(VoiceModel)
                    .filter(VoiceModel.id == voice_id, VoiceModel.prefix == prefix)
                    .first()
                )
                if existing:
                    existing.url = base_url
                    existing.extra_kwargs = extra_kwargs
                else:
                    db.add(VoiceModel(id=voice_id, url=base_url, extra_kwargs=extra_kwargs, prefix=prefix))
                added.append(voice_id)
            db.commit()
        finally:
            db.close()

    redis = _get_redis()
    if redis is not None:
        await redis.delete(f'voices_{prefix}')

    return added


async def delete_voice_by_name(voice_name: str) -> bool:
    """Delete a voice by name. Returns True if deleted."""
    prefix = config.name_prefix
    deleted = False

    if uses_mongo():
        mongo_db = get_mongo_db()
        result = await mongo_db.speech_voices.delete_one({'id': voice_name, 'prefix': prefix})
        deleted = result.deleted_count > 0
    else:
        db = SessionLocal()
        try:
            voice = (
                db.query(VoiceModel)
                .filter(VoiceModel.id == voice_name, VoiceModel.prefix == prefix)
                .first()
            )
            if voice:
                db.delete(voice)
                db.commit()
                deleted = True
        finally:
            db.close()

    if deleted:
        redis = _get_redis()
        if redis is not None:
            await redis.delete(f'voice_{prefix}_{voice_name}', f'voices_{prefix}')

    return deleted


async def delete_voices_by_url(url: str) -> int:
    """Delete all voices by URL. Returns count of deleted voices."""
    prefix = config.name_prefix
    count = 0

    if uses_mongo():
        mongo_db = get_mongo_db()
        result = await mongo_db.speech_voices.delete_many({'url': url, 'prefix': prefix})
        count = result.deleted_count
    else:
        db = SessionLocal()
        try:
            voices = (
                db.query(VoiceModel)
                .filter(VoiceModel.url == url, VoiceModel.prefix == prefix)
                .all()
            )
            count = len(voices)
            for v in voices:
                db.delete(v)
            db.commit()
        finally:
            db.close()

    if count > 0:
        redis = _get_redis()
        if redis is not None:
            await redis.delete(f'voices_{prefix}')

    return count
