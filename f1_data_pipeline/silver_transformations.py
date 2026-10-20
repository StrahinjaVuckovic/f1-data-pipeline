import os
import sys
from sqlalchemy import text

from dotenv import load_dotenv
load_dotenv()

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from f1_data_pipeline.bronze_ingestion import get_engine
from f1_data_pipeline.utils.logger import get_pipeline_logger


def execute_sql(engine, query: str) -> None:
    with engine.begin() as connection:
        connection.execute(text(query))


def truncate_silver_tables(engine, logger) -> None:
    logger.info("Truncating existing data in Silver layer...")
    truncate_query = """
        TRUNCATE TABLE 
            silver.results,
            silver.lap_times,
            silver.pit_stops,
            silver.race,
            silver.driver,
            silver.constructor,
            silver.circuit,
            silver.status,
            silver.date;
    """
    execute_sql(engine, truncate_query)


def load_silver_date(engine, logger) -> None:
    logger.info("Populating silver.date (Generating full calendar 1950-2030)")
    query = """
    INSERT INTO silver.date (
        date_id, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend
    )
    SELECT 
        TO_CHAR(d, 'YYYYMMDD')::INT AS date_id,
        d::DATE AS full_date,
        EXTRACT(YEAR FROM d)::INT AS year,
        EXTRACT(QUARTER FROM d)::INT AS quarter,
        EXTRACT(MONTH FROM d)::INT AS month,
        TO_CHAR(d, 'Month') AS month_name,
        EXTRACT(DAY FROM d)::INT AS day,
        EXTRACT(ISODOW FROM d)::INT AS day_of_week,
        TO_CHAR(d, 'Day') AS day_name,
        CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
    FROM generate_series(
        '1950-01-01'::DATE, 
        '2030-12-31'::DATE, 
        '1 day'::INTERVAL
    ) AS d
    ON CONFLICT (date_id) DO NOTHING;
    """
    execute_sql(engine, query)


def load_silver_status(engine, logger) -> None:
    logger.info("Populating silver.status")
    query = """
    INSERT INTO silver.status (status_id, status)
    SELECT DISTINCT
        "statusId"::INT,
        "status"
    FROM bronze.raw_f1_data
    WHERE "statusId" IS NOT NULL 
      AND "statusId"::TEXT != '\\N'
    ON CONFLICT (status_id) DO NOTHING;
    """
    execute_sql(engine, query)


def load_silver_circuit(engine, logger) -> None:
    logger.info("Populating silver.circuit")
    query = """
    INSERT INTO silver.circuit (
        circuit_id, circuit_ref, circuit_name, location, country, lat, lng, alt, url
    )
    SELECT DISTINCT
        "circuitId"::INT,
        "circuitRef",
        "name_y" AS circuit_name,
        "location",
        "country",
        NULLIF("lat"::TEXT, '\\N')::FLOAT,
        NULLIF("lng"::TEXT, '\\N')::FLOAT,
        NULLIF("alt"::TEXT, '\\N')::FLOAT,
        "url_y" AS url
    FROM bronze.raw_f1_data
    WHERE "circuitId" IS NOT NULL 
      AND "circuitId"::TEXT != '\\N'
    ON CONFLICT (circuit_id) DO NOTHING;
    """
    execute_sql(engine, query)


def load_silver_constructor(engine, logger) -> None:
    logger.info("Populating silver.constructor")
    query = """
    INSERT INTO silver.constructor (
        constructor_id, constructor_ref, constructor_name, nationality, url
    )
    SELECT DISTINCT
        "constructorId"::INT,
        "constructorRef",
        "name" AS constructor_name,
        "nationality_constructors" AS nationality,
        "url_constructors" AS url
    FROM bronze.raw_f1_data
    WHERE "constructorId" IS NOT NULL 
      AND "constructorId"::TEXT != '\\N'
    ON CONFLICT (constructor_id) DO NOTHING;
    """
    execute_sql(engine, query)


def load_silver_driver(engine, logger) -> None:
    logger.info("Populating silver.driver")
    query = """
    INSERT INTO silver.driver (
        driver_id, driver_ref, number, code, forename, surname, dob, nationality, url
    )
    SELECT DISTINCT
        "driverId"::INT,
        "driverRef",
        NULLIF("number_drivers"::TEXT, '\\N'),
        NULLIF("code"::TEXT, '\\N'),
        "forename",
        "surname",
        NULLIF("dob"::TEXT, '\\N')::DATE,
        "nationality",
        "url"
    FROM bronze.raw_f1_data
    WHERE "driverId" IS NOT NULL 
      AND "driverId"::TEXT != '\\N'
    ON CONFLICT (driver_id) DO NOTHING;
    """
    execute_sql(engine, query)


def load_silver_race(engine, logger) -> None:
    logger.info("Populating silver.race")
    query = """
    INSERT INTO silver.race (
        race_id, circuit_id, date_id, round, race_name, year, date, time, url,
        fp1_date, fp1_time, fp2_date, fp2_time, fp3_date, fp3_time,
        quali_date, quali_time, sprint_date, sprint_time
    )
    SELECT DISTINCT
        "raceId"::INT,
        "circuitId"::INT,
        TO_CHAR("date"::DATE, 'YYYYMMDD')::INT AS date_id,
        "round"::INT,
        "name_x" AS race_name,
        "year"::INT,
        "date"::DATE,
        NULLIF("time_races"::TEXT, '\\N'),
        "url_x" AS url,
        NULLIF("fp1_date"::TEXT, '\\N')::DATE,
        NULLIF("fp1_time"::TEXT, '\\N'),
        NULLIF("fp2_date"::TEXT, '\\N')::DATE,
        NULLIF("fp2_time"::TEXT, '\\N'),
        NULLIF("fp3_date"::TEXT, '\\N')::DATE,
        NULLIF("fp3_time"::TEXT, '\\N'),
        NULLIF("quali_date"::TEXT, '\\N')::DATE,
        NULLIF("quali_time"::TEXT, '\\N'),
        NULLIF("sprint_date"::TEXT, '\\N')::DATE,
        NULLIF("sprint_time"::TEXT, '\\N')
    FROM bronze.raw_f1_data
    WHERE "raceId" IS NOT NULL 
      AND "raceId"::TEXT != '\\N'
    ON CONFLICT (race_id) DO NOTHING;
    """
    execute_sql(engine, query)


def load_silver_results(engine, logger) -> None:
    logger.info("Populating silver.results")
    query = """
    INSERT INTO silver.results (
        result_id, race_id, driver_id, constructor_id, status_id,
        number, grid, position, position_text, position_order,
        points, laps, time, milliseconds, fastest_lap, rank,
        fastest_lap_time, fastest_lap_speed
    )
    SELECT DISTINCT
        "resultId"::INT,
        "raceId"::INT,
        "driverId"::INT,
        "constructorId"::INT,
        "statusId"::INT,
        NULLIF("number"::TEXT, '\\N')::INT,
        NULLIF("grid"::TEXT, '\\N')::INT,
        NULLIF("position"::TEXT, '\\N')::FLOAT,
        NULLIF("positionText"::TEXT, '\\N'),
        NULLIF("positionOrder"::TEXT, '\\N')::INT,
        NULLIF("points"::TEXT, '\\N')::FLOAT,
        NULLIF("laps"::TEXT, '\\N')::INT,
        NULLIF("time"::TEXT, '\\N'),
        NULLIF("milliseconds"::TEXT, '\\N')::FLOAT,
        NULLIF("fastestLap"::TEXT, '\\N')::INT,
        NULLIF("rank"::TEXT, '\\N')::INT,
        NULLIF("fastestLapTime"::TEXT, '\\N'),
        NULLIF("fastestLapSpeed"::TEXT, '\\N')::FLOAT
    FROM bronze.raw_f1_data
    WHERE "resultId" IS NOT NULL 
      AND "resultId"::TEXT != '\\N'
    ON CONFLICT (result_id) DO NOTHING;
    """
    execute_sql(engine, query)


def load_silver_lap_times(engine, logger) -> None:
    logger.info("Populating silver.lap_times")
    query = """
    INSERT INTO silver.lap_times (
        lap_time_id, race_id, driver_id, lap, position, time, milliseconds
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY "raceId"::INT, "driverId"::INT, "lap"::INT) AS lap_time_id,
        "raceId"::INT,
        "driverId"::INT,
        "lap"::INT,
        NULLIF("position_laptimes"::TEXT, '\\N')::INT,
        NULLIF("time_laptimes"::TEXT, '\\N'),
        NULLIF("milliseconds_laptimes"::TEXT, '\\N')::FLOAT
    FROM (
        SELECT DISTINCT
            "raceId", "driverId", "lap", "position_laptimes", "time_laptimes", "milliseconds_laptimes"
        FROM bronze.raw_f1_data
        WHERE "raceId" IS NOT NULL AND "raceId"::TEXT != '\\N'
          AND "driverId" IS NOT NULL AND "driverId"::TEXT != '\\N'
          AND "lap" IS NOT NULL AND "lap"::TEXT != '\\N'
    ) sub;
    """
    execute_sql(engine, query)


def load_silver_pit_stops(engine, logger) -> None:
    logger.info("Populating silver.pit_stops")
    query = """
    INSERT INTO silver.pit_stops (
        pit_stop_id, race_id, driver_id, stop, lap, time, duration, milliseconds
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY "raceId"::INT, "driverId"::INT, "stop"::INT) AS pit_stop_id,
        "raceId"::INT,
        "driverId"::INT,
        "stop"::INT,
        "lap_pitstops"::INT,
        NULLIF("time_pitstops"::TEXT, '\\N'),
        NULLIF("duration"::TEXT, '\\N'),
        NULLIF("milliseconds_pitstops"::TEXT, '\\N')::FLOAT
    FROM (
        SELECT DISTINCT
            "raceId", "driverId", "stop", "lap_pitstops", "time_pitstops", "duration", "milliseconds_pitstops"
        FROM bronze.raw_f1_data
        WHERE "raceId" IS NOT NULL AND "raceId"::TEXT != '\\N'
          AND "driverId" IS NOT NULL AND "driverId"::TEXT != '\\N'
          AND "stop" IS NOT NULL AND "stop"::TEXT != '\\N'
    ) sub;
    """
    execute_sql(engine, query)


def transform_silver_layer() -> None:
    logger = get_pipeline_logger("silver_layer")
    logger.info("Starting Silver layer ELT transformations via SQL...")

    try:
        engine = get_engine()

        truncate_silver_tables(engine, logger)

        load_silver_date(engine, logger)
        load_silver_status(engine, logger)
        load_silver_circuit(engine, logger)
        load_silver_constructor(engine, logger)
        load_silver_driver(engine, logger)

        load_silver_race(engine, logger)

        load_silver_results(engine, logger)
        load_silver_lap_times(engine, logger)
        load_silver_pit_stops(engine, logger)

        logger.info("Silver layer populated successfully via SQL ELT!")

    except Exception as e:
        logger.error(f"Error during Silver layer transformation: {str(e)}")
        raise


if __name__ == "__main__":
    transform_silver_layer()