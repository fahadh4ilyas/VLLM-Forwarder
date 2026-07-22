import os
from sqlalchemy import create_engine, Column, String, JSON, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from .config import config

WORK_DIR = os.path.abspath(os.getcwd())

DB_DIR = os.path.join(WORK_DIR, 'db')
os.makedirs(DB_DIR, exist_ok=True)

DB_PATH = os.path.join(DB_DIR, 'vllm_routes.db')
SQLALCHEMY_DATABASE_URL = f"sqlite:///{DB_PATH}"

# check_same_thread=False is needed for SQLite in FastAPI
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# ==========================================
# SQLAlchemy Models
# ==========================================

class VllmModel(Base):
    __tablename__ = "vllm_models"

    id = Column(String, primary_key=True, index=True)
    url = Column(String, nullable=False)
    properties = Column(JSON, nullable=False)
    prefix = Column(String, nullable=False, default='')

class VoiceModel(Base):
    __tablename__ = "speech_voices"
    id = Column(String, primary_key=True, index=True)
    url = Column(String, nullable=False)
    extra_kwargs = Column(JSON, nullable=False, default=list)
    prefix = Column(String, nullable=False, default='')

class UserAuth(Base):
    __tablename__ = "user_auth"

    api_key = Column(String, primary_key=True, index=True)
    response_data = Column(JSON, nullable=False)
    prefix = Column(String, nullable=False, default='')

class AgentAuth(Base):
    __tablename__ = "agent_auth"

    agent_api_key = Column(String, primary_key=True, index=True)
    agent_name = Column(String, nullable=False)
    agent_description = Column(Text, nullable=True)
    user_api_key = Column(String, nullable=False, index=True)
    prefix = Column(String, nullable=False, default='')

class ForwardRoute(Base):
    __tablename__ = "forward_routes"

    user_api_key = Column(String, primary_key=True, index=True)
    url = Column(String, nullable=False)
    prefix = Column(String, nullable=False, default='')

# ==========================================
# MongoDB Client (optional)
# ==========================================

_mongo_client: AsyncIOMotorClient | None = None
_mongo_db: AsyncIOMotorDatabase | None = None

def get_mongo_client() -> AsyncIOMotorClient | None:
    global _mongo_client
    if _mongo_client is not None:
        return _mongo_client
    if config.mongo_uri:
        _mongo_client = AsyncIOMotorClient(config.mongo_uri)
    return _mongo_client

def get_mongo_db() -> AsyncIOMotorDatabase | None:
    global _mongo_db
    if _mongo_db is not None:
        return _mongo_db
    client = get_mongo_client()
    if client and config.mongo_db:
        _mongo_db = client[config.mongo_db]
    return _mongo_db

def uses_mongo() -> bool:
    return config.mongo_uri is not None and config.mongo_db is not None

