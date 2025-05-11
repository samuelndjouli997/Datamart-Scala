-- Base : Data Mart
-- Suppose que tu as une table yellow_tripdata_2024_10 dans le serveur Data Warehouse

-- 1. DIMENSION TEMPS
INSERT INTO dim_time (datetime, year, month, day, hour)
SELECT DISTINCT tpep_pickup_datetime,
                EXTRACT(YEAR FROM tpep_pickup_datetime),
                EXTRACT(MONTH FROM tpep_pickup_datetime),
                EXTRACT(DAY FROM tpep_pickup_datetime),
                EXTRACT(HOUR FROM tpep_pickup_datetime)
FROM yellow_tripdata_2024_10;

-- Répéter pour dropoff_datetime
INSERT INTO dim_time (datetime, year, month, day, hour)
SELECT DISTINCT tpep_dropoff_datetime,
                EXTRACT(YEAR FROM tpep_dropoff_datetime),
                EXTRACT(MONTH FROM tpep_dropoff_datetime),
                EXTRACT(DAY FROM tpep_dropoff_datetime),
                EXTRACT(HOUR FROM tpep_dropoff_datetime)
FROM yellow_tripdata_2024_10
WHERE tpep_dropoff_datetime NOT IN (SELECT datetime FROM dim_time);

-- 2. DIMENSION LOCATION
INSERT INTO dim_location (location_id)
SELECT DISTINCT PULocationID FROM yellow_tripdata_2024_10
WHERE PULocationID IS NOT NULL;

INSERT INTO dim_location (location_id)
SELECT DISTINCT DOLocationID FROM yellow_tripdata_2024_10
WHERE DOLocationID IS NOT NULL
  AND DOLocationID NOT IN (SELECT location_id FROM dim_location);

-- 3. DIMENSION PAYMENT
INSERT INTO dim_payment (payment_type)
SELECT DISTINCT payment_type FROM yellow_tripdata_2024_10;

-- 4. TABLE DE FAITS
INSERT INTO fact_tripdata (
    pickup_time_id,
    dropoff_time_id,
    pickup_location_id,
    dropoff_location_id,
    payment_type,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount
)
SELECT
    pu.time_id,
    do.time_id,
    yd.PULocationID,
    yd.DOLocationID,
    yd.payment_type,
    yd.passenger_count,
    yd.trip_distance,
    yd.fare_amount,
    yd.total_amount
FROM yellow_tripdata_2024_10 yd
         JOIN dim_time pu ON pu.datetime = yd.tpep_pickup_datetime
         JOIN dim_time do ON do.datetime = yd.tpep_dropoff_datetime;
