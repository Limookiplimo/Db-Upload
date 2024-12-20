from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os

load_dotenv()

db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": os.getenv("DB_PORT")
}
encoded_username = quote_plus(db_config["user"])
encoded_password = quote_plus(db_config["password"])

SQLALCHEMY_DATABASE_URL = f'mysql+mysqlconnector://{encoded_username}:{encoded_password}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@contextmanager
def db_connection():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()