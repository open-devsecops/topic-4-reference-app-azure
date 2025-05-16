CREATE TABLE dbo.taxi_zone_lookup (
 LocationID INT,
 Borough NVARCHAR(50),
 Zone NVARCHAR(100),
 ServiceZone NVARCHAR(50)
);

----------------------------------
COPY INTO dbo.taxi_zone_lookup
FROM 'https://nyctaxistoragedataops.blob.core.windows.net/nyc-taxi-raw/taxi_zone_lookup.csv'
WITH (
 FILE_TYPE = 'CSV',
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 ROWTERMINATOR = '0x0A',
 MAXERRORS = 0,
 CREDENTIAL = (IDENTITY = 'Managed Identity')
);

----------------------------------
Select * from dbo.taxi_zone_lookup
order by LocationID

----------------------------------
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

----------------------------------
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

----------------------------------
CREATE VIEW dbo.vw_monthly_zone_revenue AS
SELECT
 FORMAT(pickup_datetime, 'yyyy-MM') AS trip_month,   -- eg. 2025-01
 pickup_zone,                                        
 taxi_type,                                          -- yellow / green
 COUNT(*) AS trip_count,                             
 SUM(total_amount) AS total_revenue                  
FROM dbo.fact_trip                                    
WHERE pickup_datetime IS NOT NULL
GROUP BY
 FORMAT(pickup_datetime, 'yyyy-MM'),
 pickup_zone,
 taxi_type;

----------------------------------
select * from dbo.vw_monthly_zone_revenue
order by trip_month, pickup_zone
