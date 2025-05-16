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