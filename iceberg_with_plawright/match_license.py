import os
from datetime import datetime
from pyspark.sql import SparkSession
from installer_scraper import scrape_licenses
import pandas as pd

BATCH_DIR = "/data/batches"
INSTALLER_LIST = "local.default.installers"
PERMIT_WITH_INSTALLER = "local.default.permit_results_installer"
CSV_PATH = "/extras/MasterLicenseData.csv"
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
 

if not table_exists(spark, INSTALLER_LIST):
    df = spark.read.option("header", True).csv(CSV_PATH)
    df = df.select("LicenseNo", "BusinessName") \
           .withColumnRenamed("LicenseNo", "CSLB") \
           .withColumnRenamed("BusinessName", "Installer Name")

    df.writeTo(INSTALLER_LIST).using("iceberg").createOrReplace()  # or `.create()` if you don't want to overwrite
    print(f"✅ Created Iceberg table {INSTALLER_LIST}")
else:
    print(f"ℹ️ Table {INSTALLER_LIST} already exists")

# Load existing data
permit_results_df = spark.read.format("iceberg").load("local.default.permit_results").select("permit_number", "CSLB")
permit_results_installer_df = spark.read.format("iceberg").load("local.default.permit_results_installer")
installer_list_df = spark.read.format("iceberg").load("local.default.installer_list")

# Get permits that are NOT enriched with installer name
new_permits_df = permit_results_df.join(
    permit_results_installer_df.select("permit_number"), 
    on="permit_number", 
    how="left_anti"
)

# Identify unmatched LicenseNos (no match in installer list)
distinct_license_nos = new_permits_df.select("CSLB").distinct()
unmatched_nos_df = distinct_license_nos.join(
    installer_list_df.select("CSLB"), 
    on="CSLB", 
    how="left_anti"
)

unmatched_license_nos = [row["CSLB"] for row in unmatched_nos_df.collect()]
print(f"🔍 Found {len(unmatched_license_nos)} unmatched CSLBs")

# Scrape missing installer info
# 4. Scrape missing installer info
if unmatched_license_nos:
    scraped_data = scrape_licenses(unmatched_license_nos)  # returns list of dicts
    valid_entries = [
        {"CSLB": r["CSLB"], "Installer Name": r["Installer Name"]}
        for r in scraped_data if r.get("Installer Name")
    ]
    if valid_entries:
        print(f"✅ Resolved {len(valid_entries)} new installers via scraping")
        scraped_spark_df = spark.createDataFrame(pd.DataFrame(valid_entries))
        scraped_spark_df.writeTo("local.default.installer_list").using("iceberg").append()
    else:
        print("⚠️ No new installer names resolved from scraping")

# Reload updated installer list
installer_list_df = spark.read.format("iceberg").load("local.default.installer_list")

# 6. Enrich new permits with installer info
enriched_df = new_permits_df.join(
    installer_list_df, on="CSLB", how="inner"
)

# 7. Append enriched records to permit_results_installer
enriched_df.writeTo("local.default.permit_results_installer").using("iceberg").append()
print(f"✅ Appended {enriched_df.count()} enriched permits")


spark.stop()