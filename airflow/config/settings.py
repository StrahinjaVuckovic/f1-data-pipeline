import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()

def get_db_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy database engine.
    Fetches database credentials securely from environment variables.
    
    Returns:
        Engine: SQLAlchemy Engine instance connected to the PostgreSQL database.
    """
    user: str = os.getenv("POSTGRES_USER", "f1_user")
    password: str = os.getenv("POSTGRES_PASSWORD", "f1_password")
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: str = os.getenv("POSTGRES_PORT", "5432")
    db_name: str = os.getenv("POSTGRES_DB", "f1_db")

    database_url: str = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

    return create_engine(database_url)