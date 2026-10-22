CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

SET search_path TO gold;

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    day INT,
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_status (
    status_id INT PRIMARY KEY,
    status VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_circuit (
    circuit_id INT PRIMARY KEY,
    circuit_ref VARCHAR(255),
    circuit_name VARCHAR(255),
    location VARCHAR(255),
    country VARCHAR(100),
    lat FLOAT,
    lng FLOAT,
    alt FLOAT,
    url TEXT
);

CREATE TABLE IF NOT EXISTS dim_constructor (
    constructor_id INT PRIMARY KEY,
    constructor_ref VARCHAR(255),
    constructor_name VARCHAR(255),
    nationality VARCHAR(100),
    url TEXT
);

CREATE TABLE IF NOT EXISTS dim_driver (
    driver_id INT PRIMARY KEY,
    driver_ref VARCHAR(255),
    number VARCHAR(50),
    code VARCHAR(10),
    forename VARCHAR(255),
    surname VARCHAR(255),
    dob DATE,
    nationality VARCHAR(100),
    url TEXT
);

CREATE TABLE IF NOT EXISTS dim_race (
    race_id INT PRIMARY KEY,
    circuit_id INT,
    date_id INT, 
    round INT,
    race_name VARCHAR(255),
    year INT,
    date DATE,
    time VARCHAR(50),
    url TEXT,
    fp1_date DATE,
    fp1_time VARCHAR(50),
    fp2_date DATE,
    fp2_time VARCHAR(50),
    fp3_date DATE,
    fp3_time VARCHAR(50),
    quali_date DATE,
    quali_time VARCHAR(50),
    sprint_date DATE,
    sprint_time VARCHAR(50),
    CONSTRAINT fk_race_circuit FOREIGN KEY (circuit_id) REFERENCES dim_circuit(circuit_id),
    CONSTRAINT fk_race_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

CREATE TABLE IF NOT EXISTS fact_results (
    result_id INT PRIMARY KEY,
    race_id INT,
    driver_id INT,
    constructor_id INT,
    status_id INT,
    number INT,
    grid INT,
    position FLOAT,
    position_text VARCHAR(50),
    position_order INT,
    points FLOAT,
    laps INT,
    time VARCHAR(50),
    milliseconds FLOAT,
    fastest_lap INT,
    rank INT,
    fastest_lap_time VARCHAR(50),
    fastest_lap_speed FLOAT,
    CONSTRAINT fk_fact_race FOREIGN KEY (race_id) REFERENCES dim_race(race_id),
    CONSTRAINT fk_fact_driver FOREIGN KEY (driver_id) REFERENCES dim_driver(driver_id),
    CONSTRAINT fk_fact_constructor FOREIGN KEY (constructor_id) REFERENCES dim_constructor(constructor_id),
    CONSTRAINT fk_fact_status FOREIGN KEY (status_id) REFERENCES dim_status(status_id)
);

CREATE TABLE IF NOT EXISTS fact_lap_times (
    lap_time_id INT PRIMARY KEY,
    race_id INT NOT NULL,
    driver_id INT NOT NULL,
    lap INT NOT NULL,
    position INT,
    time VARCHAR(50),
    milliseconds FLOAT,
    CONSTRAINT fk_lap_race FOREIGN KEY (race_id) REFERENCES dim_race(race_id),
    CONSTRAINT fk_lap_driver FOREIGN KEY (driver_id) REFERENCES dim_driver(driver_id)
);

CREATE TABLE IF NOT EXISTS fact_pit_stops (
    pit_stop_id INT PRIMARY KEY,
    race_id INT NOT NULL,
    driver_id INT NOT NULL,
    stop INT NOT NULL,           
    lap INT NOT NULL,            
    time VARCHAR(50),          
    duration VARCHAR(50),        
    milliseconds FLOAT,
    CONSTRAINT fk_pit_race FOREIGN KEY (race_id) REFERENCES dim_race(race_id),
    CONSTRAINT fk_pit_driver FOREIGN KEY (driver_id) REFERENCES dim_driver(driver_id)
);

SET search_path TO silver;

CREATE TABLE IF NOT EXISTS date (
    date_id INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    day INT,
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS status (
    status_id INT PRIMARY KEY,
    status VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS circuit (
    circuit_id INT PRIMARY KEY,
    circuit_ref VARCHAR(255),
    circuit_name VARCHAR(255),
    location VARCHAR(255),
    country VARCHAR(100),
    lat FLOAT,
    lng FLOAT,
    alt FLOAT,
    url TEXT
);

CREATE TABLE IF NOT EXISTS constructor (
    constructor_id INT PRIMARY KEY,
    constructor_ref VARCHAR(255),
    constructor_name VARCHAR(255),
    nationality VARCHAR(100),
    url TEXT
);

CREATE TABLE IF NOT EXISTS driver (
    driver_id INT PRIMARY KEY,
    driver_ref VARCHAR(255),
    number VARCHAR(50),
    code VARCHAR(10),
    forename VARCHAR(255),
    surname VARCHAR(255),
    dob DATE,
    nationality VARCHAR(100),
    url TEXT
);

CREATE TABLE IF NOT EXISTS race (
    race_id INT PRIMARY KEY,
    circuit_id INT,
    date_id INT, 
    round INT,
    race_name VARCHAR(255),
    year INT,
    date DATE,
    time VARCHAR(50),
    url TEXT,
    fp1_date DATE,
    fp1_time VARCHAR(50),
    fp2_date DATE,
    fp2_time VARCHAR(50),
    fp3_date DATE,
    fp3_time VARCHAR(50),
    quali_date DATE,
    quali_time VARCHAR(50),
    sprint_date DATE,
    sprint_time VARCHAR(50),
    CONSTRAINT fk_race_circuit FOREIGN KEY (circuit_id) REFERENCES circuit(circuit_id),
    CONSTRAINT fk_race_date FOREIGN KEY (date_id) REFERENCES date(date_id)
);

CREATE TABLE IF NOT EXISTS results (
    result_id INT PRIMARY KEY,
    race_id INT,
    driver_id INT,
    constructor_id INT,
    status_id INT,
    number INT,
    grid INT,
    position FLOAT,
    position_text VARCHAR(50),
    position_order INT,
    points FLOAT,
    laps INT,
    time VARCHAR(50),
    milliseconds FLOAT,
    fastest_lap INT,
    rank INT,
    fastest_lap_time VARCHAR(50),
    fastest_lap_speed FLOAT,
    CONSTRAINT fk_fact_race FOREIGN KEY (race_id) REFERENCES race(race_id),
    CONSTRAINT fk_fact_driver FOREIGN KEY (driver_id) REFERENCES driver(driver_id),
    CONSTRAINT fk_fact_constructor FOREIGN KEY (constructor_id) REFERENCES constructor(constructor_id),
    CONSTRAINT fk_fact_status FOREIGN KEY (status_id) REFERENCES status(status_id)
);

CREATE TABLE IF NOT EXISTS lap_times (
    lap_time_id INT PRIMARY KEY,
    race_id INT NOT NULL,
    driver_id INT NOT NULL,
    lap INT NOT NULL,
    position INT,
    time VARCHAR(50),
    milliseconds FLOAT,
    CONSTRAINT fk_lap_race FOREIGN KEY (race_id) REFERENCES race(race_id),
    CONSTRAINT fk_lap_driver FOREIGN KEY (driver_id) REFERENCES driver(driver_id)
);

CREATE TABLE IF NOT EXISTS pit_stops (
    pit_stop_id INT PRIMARY KEY,
    race_id INT NOT NULL,
    driver_id INT NOT NULL,
    stop INT NOT NULL,           
    lap INT NOT NULL,            
    time VARCHAR(50),          
    duration VARCHAR(50),        
    milliseconds FLOAT,
    CONSTRAINT fk_pit_race FOREIGN KEY (race_id) REFERENCES race(race_id),
    CONSTRAINT fk_pit_driver FOREIGN KEY (driver_id) REFERENCES driver(driver_id)
);

CREATE SCHEMA IF NOT EXISTS bronze_api;

CREATE TABLE IF NOT EXISTS bronze_api.raw_data (
    id SERIAL PRIMARY KEY,
    endpoint VARCHAR(255) NOT NULL,
    api_payload JSONB NOT NULL,
    is_processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);