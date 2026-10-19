import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()


def get_db_engine() -> Engine:
    """Creates and returns a SQLAlchemy database engine using environment variables.
    """
    user: str | None = os.getenv("POSTGRES_USER")
    password: str | None = os.getenv("POSTGRES_PASSWORD")
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: str = os.getenv("POSTGRES_PORT", "5432")
    db_name: str | None = os.getenv("POSTGRES_DB")

    if not all([user, password, db_name]):
        raise ValueError(
            "Missing required database environment variables in .env file."
        )

    database_url: str = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

    return create_engine(database_url)