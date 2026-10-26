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
    FROM generate_series('1950-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS d
    ON CONFLICT (date_id) DO NOTHING;
    """
    execute_sql(engine, query)


def load_and_enrich_silver_status(engine, logger) -> None:
    logger.info("Populating silver.status (CSV + API Full Outer Merge)")
    query = """
    WITH max_csv AS (
        SELECT COALESCE(MAX("statusId"::INT), 0) AS max_id FROM bronze.raw_f1_data
    )
    INSERT INTO silver.status (status_id, status)
    SELECT DISTINCT
        COALESCE(
            csv."statusId"::INT, 
            (SELECT max_id FROM max_csv) + DENSE_RANK() OVER (ORDER BY api.api_payload->>'status')::INT
        ) AS status_id,
        COALESCE(api.api_payload->>'status', csv."status") AS status
    FROM bronze.raw_f1_data csv
    FULL OUTER JOIN bronze_api.raw_data api 
        ON csv."status" = api.api_payload->>'status' 
        AND api.endpoint = 'status'
    WHERE COALESCE(csv."status", api.api_payload->>'status') IS NOT NULL 
      AND COALESCE(csv."status", api.api_payload->>'status') != '\\N'
    ON CONFLICT (status_id) DO UPDATE SET 
        status = EXCLUDED.status;
    """
    execute_sql(engine, query)


def load_and_enrich_silver_circuit(engine, logger) -> None:
    logger.info("Populating silver.circuit (CSV + API Full Outer Merge)")
    query = """
    WITH max_csv AS (
        SELECT COALESCE(MAX("circuitId"::INT), 0) AS max_id FROM bronze.raw_f1_data
    )
    INSERT INTO silver.circuit (
        circuit_id, circuit_ref, circuit_name, location, country, lat, lng, alt, url
    )
    SELECT DISTINCT
        COALESCE(
            csv."circuitId"::INT, 
            (SELECT max_id FROM max_csv) + DENSE_RANK() OVER (ORDER BY api.api_payload->>'circuitId')::INT
        ) AS circuit_id,
        COALESCE(csv."circuitRef", api.api_payload->>'circuitId') AS circuit_ref,
        COALESCE(api.api_payload->>'circuitName', csv."name_y") AS circuit_name,
        COALESCE(api.api_payload->'Location'->>'locality', csv."location") AS location,
        COALESCE(api.api_payload->'Location'->>'country', csv."country") AS country,
        COALESCE((api.api_payload->'Location'->>'lat')::FLOAT, NULLIF(csv."lat"::TEXT, '\\N')::FLOAT) AS lat,
        COALESCE((api.api_payload->'Location'->>'long')::FLOAT, NULLIF(csv."lng"::TEXT, '\\N')::FLOAT) AS lng,
        NULLIF(csv."alt"::TEXT, '\\N')::FLOAT AS alt,
        COALESCE(api.api_payload->>'url', csv."url_y") AS url
    FROM bronze.raw_f1_data csv
    FULL OUTER JOIN bronze_api.raw_data api 
        ON csv."circuitRef" = api.api_payload->>'circuitId' 
        AND api.endpoint = 'circuits'
    WHERE COALESCE(csv."circuitId"::TEXT, api.api_payload->>'circuitId') IS NOT NULL 
      AND COALESCE(csv."circuitId"::TEXT, api.api_payload->>'circuitId') != '\\N'
    ON CONFLICT (circuit_id) DO UPDATE SET 
        circuit_name = EXCLUDED.circuit_name,
        location = EXCLUDED.location,
        country = EXCLUDED.country,
        lat = EXCLUDED.lat,
        lng = EXCLUDED.lng,
        url = EXCLUDED.url;
    """
    execute_sql(engine, query)


def load_and_enrich_silver_constructor(engine, logger) -> None:
    logger.info("Populating silver.constructor (CSV + API Full Outer Merge)")
    query = """
    WITH max_csv AS (
        SELECT COALESCE(MAX("constructorId"::INT), 0) AS max_id FROM bronze.raw_f1_data
    )
    INSERT INTO silver.constructor (
        constructor_id, constructor_ref, constructor_name, nationality, url
    )
    SELECT DISTINCT
        COALESCE(
            csv."constructorId"::INT, 
            (SELECT max_id FROM max_csv) + DENSE_RANK() OVER (ORDER BY api.api_payload->>'constructorId')::INT
        ) AS constructor_id,
        COALESCE(csv."constructorRef", api.api_payload->>'constructorId') AS constructor_ref,
        COALESCE(api.api_payload->>'name', csv."name") AS constructor_name,
        COALESCE(api.api_payload->>'nationality', csv."nationality_constructors") AS nationality,
        COALESCE(api.api_payload->>'url', csv."url_constructors") AS url
    FROM bronze.raw_f1_data csv
    FULL OUTER JOIN bronze_api.raw_data api 
        ON csv."constructorRef" = api.api_payload->>'constructorId' 
        AND api.endpoint = 'constructors'
    WHERE COALESCE(csv."constructorId"::TEXT, api.api_payload->>'constructorId') IS NOT NULL 
      AND COALESCE(csv."constructorId"::TEXT, api.api_payload->>'constructorId') != '\\N'
    ON CONFLICT (constructor_id) DO UPDATE SET 
        constructor_name = EXCLUDED.constructor_name,
        nationality = EXCLUDED.nationality,
        url = EXCLUDED.url;
    """
    execute_sql(engine, query)


def load_and_enrich_silver_driver(engine, logger) -> None:
    logger.info("Populating silver.driver (CSV + API Full Outer Merge)")
    query = """
    WITH max_csv AS (
        SELECT COALESCE(MAX("driverId"::INT), 0) AS max_id FROM bronze.raw_f1_data
    )
    INSERT INTO silver.driver (
        driver_id, driver_ref, number, code, forename, surname, dob, nationality, url
    )
    SELECT DISTINCT
        COALESCE(
            csv."driverId"::INT, 
            (SELECT max_id FROM max_csv) + DENSE_RANK() OVER (ORDER BY api.api_payload->>'driverId')::INT
        ) AS driver_id,
        COALESCE(csv."driverRef", api.api_payload->>'driverId') AS driver_ref,
        COALESCE(api.api_payload->>'permanentNumber', NULLIF(csv."number_drivers"::TEXT, '\\N')) AS number,
        COALESCE(api.api_payload->>'code', NULLIF(csv."code"::TEXT, '\\N')) AS code,
        COALESCE(api.api_payload->>'givenName', csv."forename") AS forename,
        COALESCE(api.api_payload->>'familyName', csv."surname") AS surname,
        COALESCE((api.api_payload->>'dateOfBirth')::DATE, NULLIF(csv."dob"::TEXT, '\\N')::DATE) AS dob,
        COALESCE(api.api_payload->>'nationality', csv."nationality") AS nationality,
        COALESCE(api.api_payload->>'url', csv."url") AS url
    FROM bronze.raw_f1_data csv
    FULL OUTER JOIN bronze_api.raw_data api 
        ON csv."driverRef" = api.api_payload->>'driverId' 
        AND api.endpoint = 'drivers'
    WHERE COALESCE(csv."driverId"::TEXT, api.api_payload->>'driverId') IS NOT NULL 
      AND COALESCE(csv."driverId"::TEXT, api.api_payload->>'driverId') != '\\N'
    ON CONFLICT (driver_id) DO UPDATE SET 
        number = EXCLUDED.number,
        code = EXCLUDED.code,
        forename = EXCLUDED.forename,
        surname = EXCLUDED.surname,
        nationality = EXCLUDED.nationality,
        url = EXCLUDED.url;
    """
    execute_sql(engine, query)


def load_and_enrich_silver_race(engine, logger) -> None:
    logger.info("Populating silver.race (CSV + API Full Outer Merge)")
    query = """
    WITH max_csv AS (
        SELECT COALESCE(MAX("raceId"::INT), 0) AS max_id FROM bronze.raw_f1_data
    )
    INSERT INTO silver.race (
        race_id, circuit_id, date_id, round, race_name, year, date, time, url,
        fp1_date, fp1_time, fp2_date, fp2_time, fp3_date, fp3_time,
        quali_date, quali_time, sprint_date, sprint_time
    )
    SELECT DISTINCT
        COALESCE(
            csv."raceId"::INT, 
            (SELECT max_id FROM max_csv) + DENSE_RANK() OVER (ORDER BY api.api_payload->>'season', (api.api_payload->>'round')::INT)::INT
        ) AS race_id,
        COALESCE(csv."circuitId"::INT, c.circuit_id) AS circuit_id,
        TO_CHAR(COALESCE((api.api_payload->>'date')::DATE, csv."date"::DATE), 'YYYYMMDD')::INT AS date_id,
        COALESCE(csv."round"::INT, (api.api_payload->>'round')::INT) AS round,
        COALESCE(api.api_payload->>'raceName', csv."name_x") AS race_name,
        COALESCE(csv."year"::INT, (api.api_payload->>'season')::INT) AS year,
        COALESCE((api.api_payload->>'date')::DATE, csv."date"::DATE) AS date,
        COALESCE(api.api_payload->>'time', NULLIF(csv."time_races"::TEXT, '\\N')) AS time,
        COALESCE(api.api_payload->>'url', csv."url_x") AS url,
        COALESCE((api.api_payload->'FirstPractice'->>'date')::DATE, NULLIF(csv."fp1_date"::TEXT, '\\N')::DATE) AS fp1_date,
        COALESCE(api.api_payload->'FirstPractice'->>'time', NULLIF(csv."fp1_time"::TEXT, '\\N')) AS fp1_time,
        COALESCE((api.api_payload->'SecondPractice'->>'date')::DATE, NULLIF(csv."fp2_date"::TEXT, '\\N')::DATE) AS fp2_date,
        COALESCE(api.api_payload->'SecondPractice'->>'time', NULLIF(csv."fp2_time"::TEXT, '\\N')) AS fp2_time,
        COALESCE((api.api_payload->'ThirdPractice'->>'date')::DATE, NULLIF(csv."fp3_date"::TEXT, '\\N')::DATE) AS fp3_date,
        COALESCE(api.api_payload->'ThirdPractice'->>'time', NULLIF(csv."fp3_time"::TEXT, '\\N')) AS fp3_time,
        COALESCE((api.api_payload->'Qualifying'->>'date')::DATE, NULLIF(csv."quali_date"::TEXT, '\\N')::DATE) AS quali_date,
        COALESCE(api.api_payload->'Qualifying'->>'time', NULLIF(csv."quali_time"::TEXT, '\\N')) AS quali_time,
        NULLIF(csv."sprint_date"::TEXT, '\\N')::DATE AS sprint_date,
        NULLIF(csv."sprint_time"::TEXT, '\\N') AS sprint_time
    FROM bronze.raw_f1_data csv
    FULL OUTER JOIN bronze_api.raw_data api 
        ON csv."year"::TEXT = api.api_payload->>'season' 
        AND csv."round"::TEXT = api.api_payload->>'round'
        AND api.endpoint = 'races'
    LEFT JOIN silver.circuit c 
        ON c.circuit_ref = COALESCE(csv."circuitRef", api.api_payload->'Circuit'->>'circuitId')
    WHERE COALESCE(csv."raceId"::TEXT, api.api_payload->>'season') IS NOT NULL 
    ON CONFLICT (race_id) DO UPDATE SET 
        race_name = EXCLUDED.race_name,
        date = EXCLUDED.date,
        time = EXCLUDED.time,
        url = EXCLUDED.url;
    """
    execute_sql(engine, query)


def load_and_enrich_silver_results(engine, logger) -> None:
    logger.info("Populating silver.results (CSV + API)")
    query = """
    WITH max_csv AS (
        SELECT COALESCE(MAX("resultId"::INT), 0) AS max_id FROM bronze.raw_f1_data
    )
    INSERT INTO silver.results (
        result_id, race_id, driver_id, constructor_id, status_id,
        number, grid, position, position_text, position_order,
        points, laps, time, milliseconds, fastest_lap, rank,
        fastest_lap_time, fastest_lap_speed
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY race_id, driver_id) AS result_id,
        race_id, driver_id, constructor_id, status_id,
        number, grid, position, position_text, position_order,
        points, laps, time, milliseconds, fastest_lap, rank,
        fastest_lap_time, fastest_lap_speed
    FROM (
        -- 1. Istorijski rezultati iz CSV-a
        SELECT DISTINCT
            "resultId"::INT AS result_id_orig,
            "raceId"::INT AS race_id, 
            "driverId"::INT AS driver_id, 
            "constructorId"::INT AS constructor_id, 
            "statusId"::INT AS status_id,
            NULLIF("number"::TEXT, '\\N')::INT AS number, 
            NULLIF("grid"::TEXT, '\\N')::INT AS grid, 
            NULLIF("position"::TEXT, '\\N')::FLOAT AS position, 
            NULLIF("positionText"::TEXT, '\\N') AS position_text, 
            NULLIF("positionOrder"::TEXT, '\\N')::INT AS position_order, 
            NULLIF("points"::TEXT, '\\N')::FLOAT AS points, 
            NULLIF("laps"::TEXT, '\\N')::INT AS laps, 
            NULLIF("time"::TEXT, '\\N') AS time, 
            NULLIF("milliseconds"::TEXT, '\\N')::FLOAT AS milliseconds, 
            NULLIF("fastestLap"::TEXT, '\\N')::INT AS fastest_lap, 
            NULLIF("rank"::TEXT, '\\N')::INT AS rank, 
            NULLIF("fastestLapTime"::TEXT, '\\N') AS fastest_lap_time, 
            NULLIF("fastestLapSpeed"::TEXT, '\\N')::FLOAT AS fastest_lap_speed
        FROM bronze.raw_f1_data
        WHERE "resultId" IS NOT NULL AND "resultId"::TEXT != '\\N'

        UNION ALL

        -- 2. Novi rezultati iz API-ja
        SELECT DISTINCT
            NULL::INT AS result_id_orig,
            r.race_id,
            d.driver_id,
            c.constructor_id,
            s.status_id,
            (res_node->>'number')::INT AS number,
            (res_node->>'grid')::INT AS grid,
            (res_node->>'position')::FLOAT AS position,
            res_node->>'positionText' AS position_text,
            (res_node->>'position')::INT AS position_order,
            (res_node->>'points')::FLOAT AS points,
            (res_node->>'laps')::INT AS laps,
            res_node->'Time'->>'time' AS time,
            (res_node->'Time'->>'millis')::FLOAT AS milliseconds,
            (res_node->'FastestLap'->>'lap')::INT AS fastest_lap,
            (res_node->'FastestLap'->>'rank')::INT AS rank,
            res_node->'FastestLap'->'Time'->>'time' AS fastest_lap_time,
            (res_node->'FastestLap'->'AverageSpeed'->>'speed')::FLOAT AS fastest_lap_speed
        FROM bronze_api.raw_data b
        CROSS JOIN LATERAL jsonb_array_elements(b.api_payload->'Results') AS res_node
        JOIN silver.race r 
            ON r.year = (SPLIT_PART(b.endpoint, '/', 1))::INT 
            AND r.round = (SPLIT_PART(b.endpoint, '/', 2))::INT
        JOIN silver.driver d 
            ON d.driver_ref = res_node->'Driver'->>'driverId'
        JOIN silver.constructor c 
            ON c.constructor_ref = res_node->'Constructor'->>'constructorId'
        LEFT JOIN silver.status s 
            ON s.status = res_node->>'status'
        WHERE b.endpoint ~ '^[0-9]+/[0-9]+/results$' AND b.is_processed = FALSE
    ) combined_results
    ON CONFLICT (result_id) DO NOTHING;
    """
    execute_sql(engine, query)


def load_and_enrich_silver_lap_times(engine, logger) -> None:
    logger.info("Populating silver.lap_times (CSV + API with generated lap_time_id)")
    query = """
    INSERT INTO silver.lap_times (
        lap_time_id, race_id, driver_id, lap, position, time, milliseconds
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY race_id, driver_id, lap) AS lap_time_id,
        race_id,
        driver_id,
        lap,
        position,
        time,
        milliseconds
    FROM (
        -- 1. Istorijski krugovi iz CSV-a
        SELECT DISTINCT
            "raceId"::INT AS race_id,
            "driverId"::INT AS driver_id,
            "lap"::INT AS lap,
            NULLIF("position_laptimes"::TEXT, '\\N')::INT AS position,
            NULLIF("time_laptimes"::TEXT, '\\N') AS time,
            NULLIF("milliseconds_laptimes"::TEXT, '\\N')::FLOAT AS milliseconds
        FROM bronze.raw_f1_data
        WHERE "raceId" IS NOT NULL AND "raceId"::TEXT != '\\N'
          AND "driverId" IS NOT NULL AND "driverId"::TEXT != '\\N'
          AND "lap" IS NOT NULL AND "lap"::TEXT != '\\N'

        UNION ALL

        -- 2. Novi krugovi iz API-ja
        SELECT DISTINCT
            r.race_id,
            d.driver_id,
            (lap_node->>'number')::INT AS lap,
            (timing_node->>'position')::INT AS position,
            timing_node->>'time' AS time,
            NULL::FLOAT AS milliseconds
        FROM bronze_api.raw_data b
        CROSS JOIN LATERAL jsonb_array_elements(b.api_payload->'Laps') AS lap_node
        CROSS JOIN LATERAL jsonb_array_elements(lap_node->'Timings') AS timing_node
        JOIN silver.race r 
            ON r.year = (SPLIT_PART(b.endpoint, '/', 1))::INT 
            AND r.round = (SPLIT_PART(b.endpoint, '/', 2))::INT
        JOIN silver.driver d 
            ON d.driver_ref = timing_node->>'driverId'
        WHERE b.endpoint LIKE '%laps%' AND b.is_processed = FALSE
    ) combined_laps
    ON CONFLICT DO NOTHING;
    """
    execute_sql(engine, query)


def load_and_enrich_silver_pit_stops(engine, logger) -> None:
    logger.info("Populating silver.pit_stops (CSV + API with generated pit_stop_id)")
    query = """
    INSERT INTO silver.pit_stops (
        pit_stop_id, race_id, driver_id, stop, lap, time, duration, milliseconds
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY race_id, driver_id, stop) AS pit_stop_id,
        race_id,
        driver_id,
        stop,
        lap,
        time,
        duration,
        milliseconds
    FROM (
        -- 1. Istorijski pit stopovi iz CSV-a
        SELECT DISTINCT
            "raceId"::INT AS race_id,
            "driverId"::INT AS driver_id,
            "stop"::INT AS stop,
            "lap_pitstops"::INT AS lap,
            NULLIF("time_pitstops"::TEXT, '\\N') AS time,
            NULLIF("duration"::TEXT, '\\N') AS duration,
            NULLIF("milliseconds_pitstops"::TEXT, '\\N')::FLOAT AS milliseconds
        FROM bronze.raw_f1_data
        WHERE "raceId" IS NOT NULL AND "raceId"::TEXT != '\\N'
          AND "driverId" IS NOT NULL AND "driverId"::TEXT != '\\N'
          AND "stop" IS NOT NULL AND "stop"::TEXT != '\\N'

        UNION ALL

        -- 2. Novi pit stopovi iz API-ja
        SELECT DISTINCT
            r.race_id,
            d.driver_id,
            (pit_node->>'stop')::INT AS stop,
            (pit_node->>'lap')::INT AS lap,
            pit_node->>'time' AS time,
            pit_node->>'duration' AS duration,
            NULL::FLOAT AS milliseconds
        FROM bronze_api.raw_data b
        CROSS JOIN LATERAL jsonb_array_elements(b.api_payload->'PitStops') AS pit_node
        JOIN silver.race r 
            ON r.year = (SPLIT_PART(b.endpoint, '/', 1))::INT 
            AND r.round = (SPLIT_PART(b.endpoint, '/', 2))::INT
        JOIN silver.driver d 
            ON d.driver_ref = pit_node->>'driverId'
        WHERE b.endpoint LIKE '%pitstops%' AND b.is_processed = FALSE
    ) combined_pitstops
    ON CONFLICT DO NOTHING;
    """
    execute_sql(engine, query)


def mark_api_data_as_processed(engine, logger) -> None:
    logger.info("Marking API records in bronze_api as processed...")
    execute_sql(engine, "UPDATE bronze_api.raw_data SET is_processed = TRUE WHERE is_processed = FALSE;")


def transform_silver_layer() -> None:
    logger = get_pipeline_logger("silver_layer")
    logger.info("Starting Silver layer ELT transformations (FULL OUTER JOIN merge)...")

    try:
        engine = get_engine()

        load_silver_date(engine, logger)
        
        load_and_enrich_silver_status(engine, logger)
        load_and_enrich_silver_circuit(engine, logger)
        load_and_enrich_silver_constructor(engine, logger)
        load_and_enrich_silver_driver(engine, logger)
        load_and_enrich_silver_race(engine, logger)

        load_and_enrich_silver_results(engine, logger)
        load_and_enrich_silver_lap_times(engine, logger)
        load_and_enrich_silver_pit_stops(engine, logger)

        mark_api_data_as_processed(engine, logger)

        logger.info("Silver layer populated successfully via SQL ELT!")

    except Exception as e:
        logger.error(f"Error during Silver layer transformation: {str(e)}")
        raise


if __name__ == "__main__":
    transform_silver_layer()