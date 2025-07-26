import os
from datetime import datetime
from pyspark.sql import SparkSession

BATCH_DIR = "/data/batches"
TABLE_NAME = "local.default.permit_results"
TODAY = datetime.now().strftime("%Y%m%d")


spark = SparkSession.builder \
    .appName("AppendPermitBatches") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/data/warehouse") \
    .getOrCreate()

def is_today_batch(filename):
    try:
        timestamp_str = filename.replace("batch_", "").replace(".parquet", "")
        batch_date_str = timestamp_str.split("_")[0]  # e.g., '20250708'
        return batch_date_str == TODAY
    except Exception:
        return False

def table_exists(spark, table_name):
    try:
        spark.read.format("iceberg").load(table_name)
        return True
    except:
        return False

files = sorted(f for f in os.listdir(BATCH_DIR) if f.endswith(".parquet") and is_today_batch(f))

if not files:
    print("⚠️ No batch files to process.")
else:
    for i, file in enumerate(files):
        path = os.path.join(BATCH_DIR, file)
        print(f"📂 Checking {file}...")

        batch_df = spark.read.parquet(path)

        if i == 0 and not table_exists(spark, TABLE_NAME):
            print("🆕 Table doesn't exist. Creating...")
            batch_df.writeTo(TABLE_NAME).using("iceberg").createOrReplace()
        else:
            print("🔎 Filtering already-inserted rows by Permit Number...")
            existing_df = spark.read.format("iceberg").load(TABLE_NAME)
            new_rows = batch_df.join(existing_df, on="Permit Number", how="left_anti")

            count = new_rows.count()
            if count == 0:
                print(f"✅ No new permits found in {file}. Skipping append.")
            else:
                print(f"➕ Appending {count} new permits from {file}")
                new_rows.writeTo(TABLE_NAME).using("iceberg").append()

spark.stop()