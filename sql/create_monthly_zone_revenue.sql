CREATE TABLE dbo.monthly_zone_revenue (
  trip_month NVARCHAR(7),      
  pickup_zone NVARCHAR(255),
  taxi_type NVARCHAR(10),
  trip_count INT,
  total_revenue FLOAT
);
GO

INSERT INTO dbo.monthly_zone_revenue (trip_month, pickup_zone, taxi_type, trip_count, total_revenue)
SELECT
  FORMAT(pickup_datetime, 'yyyy-MM') AS trip_month,
  pickup_zone,
  taxi_type,
  COUNT(*) AS trip_count,
  SUM(total_amount) AS total_revenue
FROM dbo.fact_trip
WHERE pickup_datetime IS NOT NULL
GROUP BY
  FORMAT(pickup_datetime, 'yyyy-MM'),
  pickup_zone,
  taxi_type;
GO