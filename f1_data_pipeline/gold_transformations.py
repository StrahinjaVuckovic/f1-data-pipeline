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
    logger.info("Truncating fact tables and Type 1 dimensions in Gold schema...")
    truncate_query = """
        TRUNCATE TABLE 
            gold.fact_pit_stops,
            gold.fact_lap_times,
            gold.fact_results,
            gold.dim_race,
            gold.dim_circuit,
            gold.dim_status,
            gold.dim_date;
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
    logger.info("Populating gold.dim_constructor (SCD Type 2)...")
    
    close_old_query = """
        UPDATE gold.dim_constructor AS g
        SET valid_to = CURRENT_TIMESTAMP, 
            is_active = FALSE
        FROM silver.constructor AS s
        WHERE g.constructor_id = s.constructor_id
          AND g.is_active = TRUE
          AND (
              g.constructor_name IS DISTINCT FROM s.constructor_name 
              OR g.nationality IS DISTINCT FROM s.nationality
          );
    """
    execute_sql(engine, close_old_query)

    insert_new_query = """
        INSERT INTO gold.dim_constructor (
            constructor_id, constructor_ref, constructor_name, nationality, url,
            valid_from, valid_to, is_active
        )
        SELECT 
            s.constructor_id, 
            s.constructor_ref, 
            s.constructor_name, 
            s.nationality, 
            s.url,
            -- Ako konstruktor već postoji kao neaktivan, stavi trenutno vreme, inače 1900 za initial load
            CASE 
                WHEN EXISTS (SELECT 1 FROM gold.dim_constructor g WHERE g.constructor_id = s.constructor_id) 
                THEN CURRENT_TIMESTAMP 
                ELSE '1900-01-01 00:00:00'::TIMESTAMP WITH TIME ZONE 
            END AS valid_from,
            '9999-12-31 23:59:59'::TIMESTAMP WITH TIME ZONE AS valid_to,
            TRUE AS is_active
        FROM silver.constructor s
        WHERE NOT EXISTS (
            SELECT 1 FROM gold.dim_constructor g 
            WHERE g.constructor_id = s.constructor_id 
              AND g.is_active = TRUE
        );
    """
    execute_sql(engine, insert_new_query)


def load_gold_dim_driver(engine, logger) -> None:
    logger.info("Populating gold.dim_driver (SCD Type 2 with Hash Checksum)...")
    
    close_old_query = """
        UPDATE gold.dim_driver AS g
        SET valid_to = CURRENT_TIMESTAMP, 
            is_active = FALSE
        FROM silver.driver AS s
        WHERE g.driver_id = s.driver_id
          AND g.is_active = TRUE
          AND g.row_hash != s.row_hash; 
    """
    execute_sql(engine, close_old_query)

    insert_new_query = """
        INSERT INTO gold.dim_driver (
            driver_id, driver_ref, number, code, forename, surname, dob, 
            nationality, url, row_hash, valid_from, valid_to, is_active
        )
        SELECT 
            s.driver_id, 
            s.driver_ref, 
            s.number, 
            s.code, 
            s.forename, 
            s.surname, 
            s.dob, 
            s.nationality, 
            s.url, 
            s.row_hash,
            -- Ako vozač već postoji u bazi kao neaktivan, ovo je ažuriranje (CURRENT_TIMESTAMP)
            -- Ako nikada ranije nije postojao, ovo je inicijalni unos ('1900-01-01')
            CASE 
                WHEN EXISTS (SELECT 1 FROM gold.dim_driver g WHERE g.driver_id = s.driver_id) 
                THEN CURRENT_TIMESTAMP 
                ELSE '1900-01-01 00:00:00'::TIMESTAMP WITH TIME ZONE 
            END AS valid_from,
            '9999-12-31 23:59:59'::TIMESTAMP WITH TIME ZONE AS valid_to,
            TRUE AS is_active
        FROM silver.driver s
        WHERE NOT EXISTS (
            SELECT 1 FROM gold.dim_driver g 
            WHERE g.driver_id = s.driver_id 
              AND g.is_active = TRUE
        );
    """
    execute_sql(engine, insert_new_query)


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
    logger.info("Populating gold.fact_results with SCD2 lookups for constructors and drivers...")
    query = """
        INSERT INTO gold.fact_results (
            result_id, race_id, driver_sk, constructor_sk, status_id, number, grid, position,
            position_text, position_order, points, laps, time, milliseconds, fastest_lap, rank, fastest_lap_time, fastest_lap_speed
        )
        SELECT 
            sr.result_id, 
            sr.race_id, 
            dd.driver_sk,      
            dc.constructor_sk,
            sr.status_id, 
            sr.number, 
            sr.grid, 
            sr.position,
            sr.position_text, 
            sr.position_order, 
            sr.points, 
            sr.laps, 
            sr.time, 
            sr.milliseconds, 
            sr.fastest_lap, 
            sr.rank, 
            sr.fastest_lap_time, 
            sr.fastest_lap_speed
        FROM silver.results sr
        JOIN silver.race r ON sr.race_id = r.race_id
        -- SCD2 Lookup za Konstruktore
        LEFT JOIN gold.dim_constructor dc 
            ON sr.constructor_id = dc.constructor_id
            AND r.date >= dc.valid_from 
            AND r.date < dc.valid_to
        -- SCD2 Lookup za Vozače
        LEFT JOIN gold.dim_driver dd 
            ON sr.driver_id = dd.driver_id
            AND r.date >= dd.valid_from 
            AND r.date < dd.valid_to
        ON CONFLICT (result_id) DO NOTHING;
    """
    execute_sql(engine, query)


def load_gold_fact_lap_times(engine, logger) -> None:
    logger.info("Populating gold.fact_lap_times with driver SCD2 lookups...")
    query = """
        INSERT INTO gold.fact_lap_times (lap_time_id, race_id, driver_sk, lap, position, time, milliseconds)
        SELECT 
            sl.lap_time_id, 
            sl.race_id, 
            dd.driver_sk, -- Novi surogatni ključ za vozača
            sl.lap, 
            sl.position, 
            sl.time, 
            sl.milliseconds
        FROM silver.lap_times sl
        JOIN silver.race r ON sl.race_id = r.race_id
        -- SCD2 Lookup za Vozače
        LEFT JOIN gold.dim_driver dd 
            ON sl.driver_id = dd.driver_id
            AND r.date >= dd.valid_from 
            AND r.date < dd.valid_to
        ON CONFLICT (lap_time_id) DO NOTHING;
    """
    execute_sql(engine, query)


def load_gold_fact_pit_stops(engine, logger) -> None:
    logger.info("Populating gold.fact_pit_stops with driver SCD2 lookups...")
    query = """
        INSERT INTO gold.fact_pit_stops (pit_stop_id, race_id, driver_sk, stop, lap, time, duration, milliseconds)
        SELECT 
            sp.pit_stop_id, 
            sp.race_id, 
            dd.driver_sk, -- Novi surogatni ključ za vozača
            sp.stop, 
            sp.lap, 
            sp.time, 
            sp.duration, 
            sp.milliseconds
        FROM silver.pit_stops sp
        JOIN silver.race r ON sp.race_id = r.race_id
        -- SCD2 Lookup za Vozače
        LEFT JOIN gold.dim_driver dd 
            ON sp.driver_id = dd.driver_id
            AND r.date >= dd.valid_from 
            AND r.date < dd.valid_to
        ON CONFLICT (pit_stop_id) DO NOTHING;
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