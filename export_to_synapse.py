storage_account_name = dbutils.widgets.get("storage_account_name")  # Add this line
storage_account_key = dbutils.widgets.get("storage_account_key")
jdbc_username = dbutils.widgets.get("jdbc_username")
jdbc_password = dbutils.widgets.get("jdbc_password")

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

# Synapse configuration
jdbc_url = (
    "jdbc:sqlserver://<ADD_YOUR_SYNAPSE_WORKSPACE_NAME>.sql.azuresynapse.net:1433;"
    "database=nyctaxipool;"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.sql.azuresynapse.net;"
    "loginTimeout=30;"
)

temp_dir = f"wasbs://synapse-temp@{storage_account_name}.blob.core.windows.net/tempdir"

def write_to_synapse(df, table_name, processed_path):
    """Helper function to write data to Synapse"""
    # Write processed data to Synapse
    df.write \
        .format("com.databricks.spark.sqldw") \
        .option("url", jdbc_url) \
        .option("forwardSparkAzureStorageCredentials", "true") \
        .option("dbtable", table_name) \
        .option("user", jdbc_username) \
        .option("password", jdbc_password) \
        .option("tempDir", temp_dir) \
        .mode("overwrite") \
        .save()

# Process and write YELLOW data
yellow_processed_path = f"wasbs://synapse-temp@{storage_account_name}.blob.core.windows.net/processed/yellow_tripdata_2025-01.parquet"
yellow_df = spark.read.parquet(yellow_processed_path)
write_to_synapse(yellow_df, "yellow_tripdata_2025_01", yellow_processed_path)

# Process and write GREEN data
green_processed_path = f"wasbs://synapse-temp@{storage_account_name}.blob.core.windows.net/processed/green_tripdata_2025-01.parquet"
green_df = spark.read.parquet(green_processed_path)
write_to_synapse(green_df, "green_tripdata_2025_01", green_processed_path)

print("Successfully wrote both yellow and green data to Synapse!")