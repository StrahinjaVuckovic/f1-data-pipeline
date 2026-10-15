
DROP TABLE IF EXISTS fact_results CASCADE;
DROP TABLE IF EXISTS dim_race CASCADE;
DROP TABLE IF EXISTS dim_driver CASCADE;
DROP TABLE IF EXISTS dim_constructor CASCADE;
DROP TABLE IF EXISTS dim_circuit CASCADE;
DROP TABLE IF EXISTS dim_status CASCADE;


CREATE TABLE dim_status (
    status_id INT PRIMARY KEY,
    status VARCHAR(255)
);

CREATE TABLE dim_circuit (
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

CREATE TABLE dim_constructor (
    constructor_id INT PRIMARY KEY,
    constructor_ref VARCHAR(255),
    constructor_name VARCHAR(255),
    nationality VARCHAR(100),
    url TEXT
);

CREATE TABLE dim_driver (
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

CREATE TABLE dim_race (
    race_id INT PRIMARY KEY,
    circuit_id INT,
    year INT,
    round INT,
    race_name VARCHAR(255),
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
    CONSTRAINT fk_race_circuit FOREIGN KEY (circuit_id) REFERENCES dim_circuit(circuit_id)
);


CREATE TABLE fact_results (
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