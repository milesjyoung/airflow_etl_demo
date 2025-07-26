from pyspark.sql import SparkSession
from ml_extractor import extract_details_batch
import pandas as pd

spark = SparkSession.builder \
    .appName("EnrichPermitDetails") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/data/warehouse") \
    .getOrCreate()

PERMIT_RESULTS = "local.default.permit_results"
PERMIT_DETAILS = "local.default.permit_details_extracted"

# Load
permit_results_df = spark.read.format("iceberg").load(PERMIT_RESULTS).select("permit_number", "description")
permit_details_df = spark.read.format("iceberg").load(PERMIT_DETAILS).select("permit_number")

# Identify new rows
new_df = permit_results_df.join(permit_details_df, on="permit_number", how="left_anti")
rows = new_df.collect()

print(f"🧠 Extracting details for {len(rows)} new permits...")

permit_numbers = [row["permit_number"] for row in rows]
descriptions = [row["description"] for row in rows]

# Inference
extracted = extract_details_batch(descriptions)

# Combine with permit_number
records = [
    {"permit_number": permit_numbers[i], **extracted[i]}
    for i in range(len(permit_numbers))
]

if records:
    enriched_df = spark.createDataFrame(pd.DataFrame(records))
    enriched_df.writeTo(PERMIT_DETAILS).using("iceberg").append()
    print(f"✅ Appended {len(records)} extracted details.")
else:
    print("⚠️ No valid extraction results.")

spark.stop()