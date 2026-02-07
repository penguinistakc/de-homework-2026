-- CREATE OR REPLACE table nyc_taxi.yellow_tripdata_2024 AS
-- select * from `nyc_taxi.yellow_tripdata_2024_external`;

-- select count(*) from nyc_taxi.yellow_tripdata_2024;

-- select count(*) from nyc_taxi.yellow_tripdata_2024_external;

-- select count(DISTINCT PULocationID) from nyc_taxi.yellow_tripdata_2024;

-- select count(DISTINCT PULocationID) from nyc_taxi.yellow_tripdata_2024_external;

-- select PULocationID from nyc_taxi.yellow_tripdata_2024;

-- select PULocationID, DOLocationId from nyc_taxi.yellow_tripdata_2024;

-- select count(*) from nyc_taxi.yellow_tripdata_2024 where fare_amount = 0;

-- CREATE OR REPLACE TABLE nyc_taxi.yellow_tripdata_2024_partitioned_clustered
-- PARTITION BY DATE(tpep_pickup_datetime)
-- CLUSTER BY VendorID AS
-- SELECT * FROM nyc_taxi.yellow_tripdata_2024_external;

-- SELECT DISTINCT(VendorID)
-- FROM `nyc_taxi.yellow_tripdata_2024`
-- WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- SELECT DISTINCT(VendorID)
-- from `nyc_taxi.yellow_tripdata_2024_partitioned_clustered`
-- WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
