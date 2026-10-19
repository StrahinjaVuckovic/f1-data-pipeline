import os
import sys
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine, text

def get_engine():
    """Gets database engine dynamically whether running in Airflow or locally."""
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="postgres_default")
        return hook.get_sqlalchemy_engine()
    except Exception:
        
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "postgres")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "f1_db")
        return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def ingest_bronze_data(file_path: str = "/opt/airflow/data/dataEngineeringDataset.csv") -> None:
    """
    Extracts raw Formula 1 data from a CSV source file, adds technical ingestion 
    metadata, and dynamically loads it into the PostgreSQL bronze schema.
    """
    print(f"[{datetime.now(timezone.utc)}] Starting Bronze layer ingestion process...")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source CSV file not found at path: {file_path}")

    print(f"Reading source file: {file_path}")
    df = pd.read_csv(file_path, low_memory=False)

    df["ingestion_timestamp"] = datetime.now(timezone.utc)
    df["source_file"] = os.path.basename(file_path)

    engine = get_engine()

    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))

    print("Writing raw data to database table 'bronze.raw_f1_data'...")
    df.to_sql(
        name="raw_f1_data",
        con=engine,
        schema="bronze",
        if_exists="replace",
        index=False,
        chunksize=10000,
    )

    print(f"[{datetime.now(timezone.utc)}] Bronze ingestion completed successfully! Total records loaded: {len(df)}")


if __name__ == "__main__":
    ingest_bronze_data("data/dataEngineeringDataset.csv")