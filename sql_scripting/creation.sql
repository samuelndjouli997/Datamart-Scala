-- Base : Data Mart

CREATE TABLE dim_time (
                          time_id SERIAL PRIMARY KEY,
                          datetime TIMESTAMP,
                          year INT,
                          month INT,
                          day INT,
                          hour INT
);

CREATE TABLE dim_location (
                              location_id INT PRIMARY KEY,
                              location_name TEXT
);

CREATE TABLE dim_payment (
                             payment_type TEXT PRIMARY KEY,
                             description TEXT
);

CREATE TABLE fact_tripdata (
                               id SERIAL PRIMARY KEY,
                               pickup_time_id INT REFERENCES dim_time(time_id),
                               dropoff_time_id INT REFERENCES dim_time(time_id),
                               pickup_location_id INT REFERENCES dim_location(location_id),
                               dropoff_location_id INT REFERENCES dim_location(location_id),
                               payment_type TEXT REFERENCES dim_payment(payment_type),
                               passenger_count INT,
                               trip_distance FLOAT,
                               fare_amount FLOAT,
                               total_amount FLOAT
);
