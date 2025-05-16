CREATE TABLE dbo.fact_trip
WITH (
DISTRIBUTION = ROUND_ROBIN
)
AS
SELECT
 t.taxi_type,
 t.pickup_datetime,
 t.dropoff_datetime,
 t.passenger_count,
 t.trip_distance,
 t.fare_amount,
 t.tip_amount,
 t.total_amount,
 zpu.Zone AS pickup_zone,
 zpu.Borough AS pickup_borough,
 zdo.Zone AS dropoff_zone,
 zdo.Borough AS dropoff_borough
FROM (
 SELECT * FROM dbo.stg_yellow_tripdata
 UNION ALL
 SELECT * FROM dbo.stg_green_tripdata
) t
LEFT JOIN dbo.taxi_zone_lookup zpu ON t.pu_location_id = zpu.LocationID
LEFT JOIN dbo.taxi_zone_lookup zdo ON t.do_location_id = zdo.LocationID;