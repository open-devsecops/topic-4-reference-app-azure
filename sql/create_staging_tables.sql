CREATE TABLE dbo.stg_yellow_tripdata
WITH (
DISTRIBUTION = ROUND_ROBIN
)
AS
SELECT
 vendor_id,
 CAST(pickup_datetime AS DATETIME2) AS pickup_datetime,
 CAST(dropoff_datetime AS DATETIME2) AS dropoff_datetime,
 passenger_count,
 trip_distance,
 rate_code_id,
 store_and_fwd_flag,
 pu_location_id,
 do_location_id,
 payment_type,
 fare_amount,
 extra,
 mta_tax,
 tip_amount,
 tolls_amount,
 NULL AS ehail_fee,            -- not exist for yellow.make it NULL to be consisitent with green
 improvement_surcharge,
 total_amount,
 NULL AS trip_type,           -- not exist for yellow.make it NULL to be consisitent with green
 congestion_surcharge,
 airport_fee,
 'yellow' AS taxi_type
FROM dbo.yellow_tripdata_2025_01;

----------------------------------
CREATE TABLE dbo.stg_green_tripdata
WITH (
 DISTRIBUTION = ROUND_ROBIN
)
AS
SELECT
 vendor_id,
 CAST(pickup_datetime AS DATETIME2) AS pickup_datetime,
 CAST(dropoff_datetime AS DATETIME2) AS dropoff_datetime,
 passenger_count,
 trip_distance,
 rate_code_id,
 store_and_fwd_flag,
 pu_location_id,
 do_location_id,
 payment_type,
 fare_amount,
 extra,
 mta_tax,
 tip_amount,
 tolls_amount,
 ehail_fee,
 improvement_surcharge,
 total_amount,
 trip_type,
 congestion_surcharge,
 NULL AS airport_fee,-- not exist for green.make it NULL to be consisitent with yellow
 'green' AS taxi_type
FROM dbo.green_tripdata_2025_01;