CREATE TABLE dbo.taxi_zone_lookup (
 LocationID INT,
 Borough NVARCHAR(50),
 Zone NVARCHAR(100),
 ServiceZone NVARCHAR(50)
);

----------------------------------
COPY INTO dbo.taxi_zone_lookup
FROM 'https://<ADD_STORAGE_ACCOUNT_NAME>.blob.core.windows.net/nyc-taxi-raw/taxi_zone_lookup.csv'
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


