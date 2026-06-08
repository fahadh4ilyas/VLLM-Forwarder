import os
from sqlalchemy import create_engine, Column, String, JSON
from sqlalchemy.orm import sessionmaker, declarative_base

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

class VllmModel(Base):
    __tablename__ = "vllm_models"
    
    id = Column(String, primary_key=True, index=True) # e.g., "meta-llama/Llama-3-8b"
    url = Column(String, nullable=False)              # e.g., "http://192.168.1.100:8000/v1"
    properties = Column(JSON, nullable=False)         # The exact JSON from /v1/models

class VoiceModel(Base):
    __tablename__ = "speech_voices"
    id = Column(String, primary_key=True, index=True)     # e.g., "vivian"
    url = Column(String, nullable=False)                  # e.g., "http://192.168.1.100:8091/v1"
    extra_kwargs = Column(JSON, nullable=False, default=list) # e.g., ["silent_length"]

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()