import dotenv, typing
from pydantic import Field
from pydantic_settings import BaseSettings

class ApiConfig(BaseSettings):
    api_host: str = Field('127.0.0.1')
    api_port: int = Field(5000)
    worker_num: int = Field(1)
    forwarded_allow_ips: str = Field('127.0.0.1')

    class Config:
        env_file = dotenv.find_dotenv(usecwd=True)
        env_file_encoding = 'utf-8'
        extra = 'ignore'

config = ApiConfig()