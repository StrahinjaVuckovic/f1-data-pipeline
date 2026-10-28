import os
import sys
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from f1_data_pipeline.bronze_ingestion import get_engine
from f1_data_pipeline.utils.logger import get_pipeline_logger


def execute_sql(engine, query: str) -> None:
   
    with engine.begin() as connection:
        connection.execute(text(query))


def truncate_gold_tables(engine, logger) -> None:
   
    logger.info("Truncating all tables in Gold schema...")
    truncate_query = """
        TRUNCATE TABLE 
            gold.fact_pit_stops,
            gold.fact_lap_times,
            gold.fact_results,
            gold.dim_race,
            gold.dim_driver,
            gold.dim_constructor,
            gold.dim_circuit,
            gold.dim_status,
            gold.dim_date
        CASCADE;
    """
    execute_sql(engine, truncate_query)



def load_gold_dim_date(engine, logger) -> None:
    logger.info("Populating gold.dim_date...")
    query = """
        INSERT INTO gold.dim_date (date_id, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
        SELECT date_id, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend
        FROM silver.date;
    """
    execute_sql(engine, query)


def load_gold_dim_status(engine, logger) -> None:
    logger.info("Populating gold.dim_status...")
    query = """
        INSERT INTO gold.dim_status (status_id, status)
        SELECT status_id, status
        FROM silver.status;
    """
    execute_sql(engine, query)


def load_gold_dim_circuit(engine, logger) -> None:
    logger.info("Populating gold.dim_circuit...")
    query = """
        INSERT INTO gold.dim_circuit (circuit_id, circuit_ref, circuit_name, location, country, lat, lng, alt, url)
        SELECT circuit_id, circuit_ref, circuit_name, location, country, lat, lng, alt, url
        FROM silver.circuit;
    """
    execute_sql(engine, query)


def load_gold_dim_constructor(engine, logger) -> None:
    logger.info("Populating gold.dim_constructor...")
    query = """
        INSERT INTO gold.dim_constructor (constructor_id, constructor_ref, constructor_name, nationality, url)
        SELECT constructor_id, constructor_ref, constructor_name, nationality, url
        FROM silver.constructor;
    """
    execute_sql(engine, query)


def load_gold_dim_driver(engine, logger) -> None:
    logger.info("Populating gold.dim_driver...")
    query = """
        INSERT INTO gold.dim_driver (driver_id, driver_ref, number, code, forename, surname, dob, nationality, url)
        SELECT driver_id, driver_ref, number, code, forename, surname, dob, nationality, url
        FROM silver.driver;
    """
    execute_sql(engine, query)


def load_gold_dim_race(engine, logger) -> None:
    logger.info("Populating gold.dim_race...")
    query = """
        INSERT INTO gold.dim_race (
            race_id, circuit_id, date_id, round, race_name, year, date, time, url,
            fp1_date, fp1_time, fp2_date, fp2_time, fp3_date, fp3_time, quali_date, quali_time, sprint_date, sprint_time
        )
        SELECT 
            race_id, circuit_id, date_id, round, race_name, year, date, time, url,
            fp1_date, fp1_time, fp2_date, fp2_time, fp3_date, fp3_time, quali_date, quali_time, sprint_date, sprint_time
        FROM silver.race;
    """
    execute_sql(engine, query)



def load_gold_fact_results(engine, logger) -> None:
    logger.info("Populating gold.fact_results...")
    query = """
        INSERT INTO gold.fact_results (
            result_id, race_id, driver_id, constructor_id, status_id, number, grid, position,
            position_text, position_order, points, laps, time, milliseconds, fastest_lap, rank, fastest_lap_time, fastest_lap_speed
        )
        SELECT 
            result_id, race_id, driver_id, constructor_id, status_id, number, grid, position,
            position_text, position_order, points, laps, time, milliseconds, fastest_lap, rank, fastest_lap_time, fastest_lap_speed
        FROM silver.results;
    """
    execute_sql(engine, query)


def load_gold_fact_lap_times(engine, logger) -> None:
    logger.info("Populating gold.fact_lap_times...")
    query = """
        INSERT INTO gold.fact_lap_times (lap_time_id, race_id, driver_id, lap, position, time, milliseconds)
        SELECT lap_time_id, race_id, driver_id, lap, position, time, milliseconds
        FROM silver.lap_times;
    """
    execute_sql(engine, query)


def load_gold_fact_pit_stops(engine, logger) -> None:
    logger.info("Populating gold.fact_pit_stops...")
    query = """
        INSERT INTO gold.fact_pit_stops (pit_stop_id, race_id, driver_id, stop, lap, time, duration, milliseconds)
        SELECT pit_stop_id, race_id, driver_id, stop, lap, time, duration, milliseconds
        FROM silver.pit_stops;
    """
    execute_sql(engine, query)



def transform_gold_layer() -> None:
    """Glavna funkcija za izvršavanje kompletne Gold ELT transformacije."""
    logger = get_pipeline_logger("gold_transformations")
    logger.info("Starting Gold layer ELT transformation process...")

    try:
        engine = get_engine()

        truncate_gold_tables(engine, logger)

        load_gold_dim_date(engine, logger)
        load_gold_dim_status(engine, logger)
        load_gold_dim_circuit(engine, logger)
        load_gold_dim_constructor(engine, logger)
        load_gold_dim_driver(engine, logger)
        load_gold_dim_race(engine, logger)  

        load_gold_fact_results(engine, logger)
        load_gold_fact_lap_times(engine, logger)
        load_gold_fact_pit_stops(engine, logger)

        logger.info("Gold layer populated successfully via SQL ELT!")

    except Exception as e:
        logger.error(f"Error during Gold layer transformation: {str(e)}")
        raise


if __name__ == "__main__":
    transform_gold_layer()