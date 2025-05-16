storage_account_name = dbutils.widgets.get("storage_account_name")  # Add this line
storage_account_key = dbutils.widgets.get("storage_account_key")

container_name = "nyc-taxi-raw"
parquet_path_green = "green/2025/01/green_tripdata_2025-01.parquet"
parquet_path_yellow = "yellow/2025/01/yellow_tripdata_2025-01.parquet"

# configure Spark to visit Azure Blob
spark.conf.set(
 f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
 storage_account_key
)

# get the complete file path
file_path_green = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{parquet_path_green}"
file_path_yellow = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{parquet_path_yellow}"

# read parquet file
df_green = spark.read.parquet(file_path_green)
df_yellow = spark.read.parquet(file_path_yellow)

# show df data and schema
df_green.show(5)
df_yellow.show(5)
df_green.printSchema()
df_yellow.printSchema()

# Clean the Yellow taxi dataframe
# modify the column names mapping
rename_map_yellow = {
   "VendorID": "vendor_id",
   "tpep_pickup_datetime": "pickup_datetime",
   "tpep_dropoff_datetime": "dropoff_datetime",
   "passenger_count": "passenger_count",
   "trip_distance": "trip_distance",
   "RatecodeID": "rate_code_id",
   "store_and_fwd_flag": "store_and_fwd_flag",
   "PULocationID": "pu_location_id",
   "DOLocationID": "do_location_id",
   "payment_type": "payment_type",
   "fare_amount": "fare_amount",
   "extra": "extra",
   "mta_tax": "mta_tax",
   "tip_amount": "tip_amount",
   "tolls_amount": "tolls_amount",
   "improvement_surcharge": "improvement_surcharge",
   "total_amount": "total_amount",
   "congestion_surcharge": "congestion_surcharge",
   "Airport_fee": "airport_fee"
}

rename_map_green = {
   "VendorID": "vendor_id",
   "lpep_pickup_datetime": "pickup_datetime",
   "lpep_dropoff_datetime": "dropoff_datetime",
   "passenger_count": "passenger_count",
   "trip_distance": "trip_distance",
   "RatecodeID": "rate_code_id",
   "store_and_fwd_flag": "store_and_fwd_flag",
   "PULocationID": "pu_location_id",
   "DOLocationID": "do_location_id",
   "payment_type": "payment_type",
   "fare_amount": "fare_amount",
   "extra": "extra",
   "mta_tax": "mta_tax",
   "tip_amount": "tip_amount",
   "tolls_amount": "tolls_amount",
   "improvement_surcharge": "improvement_surcharge",
   "total_amount": "total_amount",
   "congestion_surcharge": "congestion_surcharge",
   "Airport_fee": "airport_fee"
}

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# rename the columns
for old_col, new_col in rename_map_green.items():
   df_green = df_green.withColumnRenamed(old_col, new_col)

# data type standardization（eg：convert passenger_count and vendor_id to Integer）
df_green = df_green.withColumn("vendor_id", col("vendor_id").cast(IntegerType()))
df_green = df_green.withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
df_green = df_green.withColumn("rate_code_id", col("rate_code_id").cast(IntegerType()))
df_green = df_green.withColumn("pu_location_id", col("pu_location_id").cast(IntegerType()))
df_green = df_green.withColumn("do_location_id", col("do_location_id").cast(IntegerType()))
df_green = df_green.withColumn("payment_type", col("payment_type").cast(IntegerType()))

# Null value handling
df_green = df_green.na.fill({"passenger_count": 0})

# check schema again
df_green.printSchema()
df_green.show(5)

processed_path_green = f"wasbs://synapse-temp@{storage_account_name}.blob.core.windows.net/processed/green_tripdata_2025-01.parquet"
df_green.write.parquet(processed_path_green, mode="overwrite")

# rename the columns
for old_col, new_col in rename_map_yellow.items():
   df_yellow = df_yellow.withColumnRenamed(old_col, new_col)

# data type standardization（eg：convert passenger_count and vendor_id to Integer）
df_yellow = df_yellow.withColumn("vendor_id", col("vendor_id").cast(IntegerType()))
df_yellow = df_yellow.withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
df_yellow = df_yellow.withColumn("rate_code_id", col("rate_code_id").cast(IntegerType()))
df_yellow = df_yellow.withColumn("pu_location_id", col("pu_location_id").cast(IntegerType()))
df_yellow = df_yellow.withColumn("do_location_id", col("do_location_id").cast(IntegerType()))
df_yellow = df_yellow.withColumn("payment_type", col("payment_type").cast(IntegerType()))

# Null value handling
df_yellow = df_yellow.na.fill({"passenger_count": 0})

# check schema again
df_yellow.printSchema()
df_yellow.show(5)

processed_path_yellow = f"wasbs://synapse-temp@{storage_account_name}.blob.core.windows.net/processed/yellow_tripdata_2025-01.parquet"
df_yellow.write.parquet(processed_path_yellow, mode="overwrite")