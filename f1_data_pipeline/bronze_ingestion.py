import os
import sys
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from f1_data_pipeline.utils.logger import get_pipeline_logger


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


def init_bronze_tables(engine) -> None:
    """Reads and executes the SQL DDL file to ensure bronze tables exist."""
    sql_file_path = os.path.join(ROOT_DIR, "f1_data_pipeline", "sql", "create_bronze_tables.sql")
    with open(sql_file_path, "r", encoding="utf-8") as file:
        ddl_query = file.read()

    with engine.begin() as connection:
        connection.execute(text(ddl_query))


def get_existing_row_count(engine) -> int:
    """Returns the current row count in the bronze table if it exists."""
    check_query = text("""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = 'bronze' AND table_name = 'raw_f1_data';
    """)
    try:
        with engine.connect() as connection:
            table_exists = connection.execute(check_query).scalar()
            if table_exists:
                count_query = text("SELECT COUNT(*) FROM bronze.raw_f1_data;")
                return connection.execute(count_query).scalar() or 0
    except Exception:
        return 0
    return 0


def ingest_bronze_data(file_path: str = "/opt/airflow/data/dataEngineeringDataset.csv") -> None:
    """
    Incrementally ingests only NEW records from the CSV file into the bronze table
    by skipping previously processed rows.
    """
    logger = get_pipeline_logger("bronze_ingestion")
    logger.info("Starting Bronze layer ingestion process...")

    if not os.path.exists(file_path):
        logger.error(f"Source CSV file not found at path: {file_path}")
        raise FileNotFoundError(f"Source CSV file not found at path: {file_path}")

    engine = get_engine()

    init_bronze_tables(engine)

    existing_rows = get_existing_row_count(engine)
    logger.info(f"Current record count in 'bronze.raw_f1_data': {existing_rows}")

    if existing_rows > 0:
        logger.info(f"Skipping first {existing_rows} data rows in source file...")
        df = pd.read_csv(file_path, skiprows=range(1, existing_rows + 1), low_memory=False)
    else:
        logger.info("Table is empty. Reading the full CSV file...")
        df = pd.read_csv(file_path, low_memory=False)

    if df.empty:
        logger.info("No new records found in CSV file. Bronze layer is up to date.")
        return

    logger.info(f"New records detected for ingestion: {len(df)}")

    df["ingestion_timestamp"] = datetime.now(timezone.utc)
    df["source_file"] = os.path.basename(file_path)

    df.to_sql(
        name="raw_f1_data",
        con=engine,
        schema="bronze",
        if_exists="append",
        index=False,
        chunksize=10000,
    )

    logger.info(f"Bronze incremental ingestion completed successfully! Rows appended: {len(df)}")


if __name__ == "__main__":
    ingest_bronze_data("data/dataEngineeringDataset.csv")