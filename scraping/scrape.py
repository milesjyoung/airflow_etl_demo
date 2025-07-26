import time
import asyncio
from datetime import date, timedelta, datetime
import os
from pathlib import Path
from playwright.async_api import async_playwright
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError
import pandas as pd
from google.cloud import storage
# import psycopg2
# from pyspark.sql import SparkSession
import pyarrow as pa
import pyarrow.parquet as pq


DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/tmp"))
DOWNLOAD_DIR.mkdir(exist_ok=True)

GCS_BUCKET = "name_bucket"
GCS_PREFIX = "name_prefix"

BASE_URL = "https://permits.nevadacountyca.gov/CitizenAccess/Cap/CapHome.aspx?module=Building&TabName=Building"

async def get_license_from_permit(page: Page, permit_number: str) -> tuple[str, str | None]:
    times = {}

    #Load Search Page
    try:
        t0 = time.time()
        await page.goto(BASE_URL, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")
        times["goto"] = time.time() - t0
    except Exception as e:
        print(f"❌ Error loading search page for {permit_number}: {e}")
        return permit_number, None

    # Submit Permit Number
    try:
        t0 = time.time()
        await page.get_by_label("Permit Number").fill(str(permit_number))
        await page.locator("#ctl00_PlaceHolderMain_btnNewSearch").click()
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(1000)  # Give the JS time to finish

        times["search"] = time.time() - t0

        if "Error.aspx" in page.url:
            print(f"❌ Redirected to error page for {permit_number}")
            return permit_number, None

        await page.wait_for_timeout(1000)
    except PlaywrightTimeoutError as e:
        print(f"⏱️ Timeout after search for {permit_number}: {e}")
        return permit_number, None
    except Exception as e:
        print(f"❌ Error during search submission for {permit_number}: {e}")
        return permit_number, None

    # Expand “More Detail”
    try:
        more_detail = page.locator("#lnkMoreDetail")
        if not await more_detail.is_visible():
            print(f"❌ More Detail link not visible for {permit_number}")
            return permit_number, None

        t0 = time.time()
        await more_detail.click()
        times["click_more_detail"] = time.time() - t0
    except Exception as e:
        print(f"❌ Error clicking More Detail for {permit_number}: {e}")
        return permit_number, None

    # Expand Application Info
    try:
        app_info = page.locator("a[title='Expand Application Information']")
        if not await app_info.is_visible():
            print(f"❌ Application Info link not visible for {permit_number}")
            return permit_number, None

        t0 = time.time()
        await app_info.click()
        times["click_app_info"] = time.time() - t0
    except Exception as e:
        print(f"❌ Error expanding application info for {permit_number}: {e}")
        return permit_number, None

    # Extract License Number
    try:
        t0 = time.time()
        label_span = page.locator(
            "xpath=//div[contains(@class, 'MoreDetail_ItemCol1')]/span[normalize-space(text())='License Number:']"
        )

        if not await label_span.is_visible(timeout=3000):
            print(f"❌ License label not found for {permit_number}")
            return permit_number, None

        label_div = label_span.locator("xpath=..")
        value_div = label_div.locator("xpath=following-sibling::div[1]")
        license_number = (await value_div.locator("span").inner_text()).strip()
        times["extract_license"] = time.time() - t0

        return permit_number, license_number
    except PlaywrightTimeoutError as e:
        print(f"⏱️ Timeout extracting license for {permit_number}: {e}")
        return permit_number, None
    except Exception as e:
        print(f"❌ Error extracting license for {permit_number}: {e}")
        return permit_number, None
    

def get_previous_month_date_range():
    today = date.today()
    first_day_current_month = today.replace(day=1)
    last_day_previous_month = first_day_current_month - timedelta(days=1)
    first_day_previous_month = last_day_previous_month.replace(day=1)
    
    return first_day_previous_month, last_day_previous_month
    

# def insert_batch_to_postgres(batch_df: pd.DataFrame):
#     try:
#         print(batch_df.head())
#         conn = psycopg2.connect(
#             dbname="airflow",
#             user="airflow",
#             password="airflow",
#             host="postgres",
#             port=5432
#         )
#         cursor = conn.cursor()
#         cursor.execute("""
#             CREATE TABLE IF NOT EXISTS permit_results_full (
#                 permit_number TEXT PRIMARY KEY,
#                 date DATE,
#                 permit_type TEXT,
#                 usage TEXT,
#                 description TEXT,
#                 address TEXT,
#                 status TEXT,
#                 installer_number TEXT
#             )
#         """)
#         for _, row in batch_df.iterrows():
#             cursor.execute("""
#                 INSERT INTO permit_results_full (
#                     permit_number, date, permit_type, usage, description,
#                     address, status, installer_number
#                 )
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#                 ON CONFLICT (permit_number) DO UPDATE
#                 SET installer_number = EXCLUDED.installer_number
#             """, (
#                 row["Permit Number"],
#                 row["Date"],
#                 row["Permit Type"],
#                 row["Usage"],
#                 row["Description"],
#                 row["Address"],
#                 row["Status"],
#                 row.get("Installer Number", None)
#             ))
#         conn.commit()
#         cursor.close()
#         conn.close()
#     except Exception as e:
#         print("❌ DB Error:", e)

# def write_batch_to_iceberg(batch_df: pd.DataFrame):
#     try:

#         spark = SparkSession.builder \
#             .appName("WritePermitBatch") \
#             .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
#             .config("spark.sql.catalog.local.type", "hadoop") \
#             .config("spark.sql.catalog.local.warehouse", "/data/warehouse") \
#             .getOrCreate()

#         sdf = spark.createDataFrame(batch_df)

#         # Write to Iceberg table (append mode)
#         sdf.writeTo("local.default.permit_results").using("iceberg").append()

#         spark.stop()
#         print(f"✅ Written batch to iceberg!")
#     except Exception as e:
#         print("❌ DB Error:", e)
# def write_batch_to_iceberg(batch_df: pd.DataFrame):
#     try:
#         spark = SparkSession.builder \
#             .appName("WritePermitBatch") \
#             .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
#             .config("spark.sql.catalog.local.type", "hadoop") \
#             .config("spark.sql.catalog.local.warehouse", "/data/warehouse") \
#             .getOrCreate()

#         sdf = spark.createDataFrame(batch_df)
#         table_name = "local.default.permit_results"

#         if not spark.catalog._jcatalog.tableExists("default.permit_results"):
#             sdf.writeTo(table_name) \
#                 .using("iceberg") \
#                 .tableProperty("format-version", "2") \
#                 .create()
#             print(f"✅ Created and wrote initial batch to table {table_name}")
#         else:
#             sdf.writeTo(table_name) \
#                 .using("iceberg") \
#                 .append()
#             print(f"✅ Appended batch to existing table {table_name}")

#         spark.stop()

#     except Exception as e:
#         print("❌ DB Error:", e)


def write_batch_to_storage(df: pd.DataFrame):
    try:
        output_dir = "/data/batches"
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_path = os.path.join(output_dir, f"batch_{timestamp}.parquet")

        df = df.copy()

        # 1. Downcast all datetime64[ns] to datetime64[ms]
        for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
            df[col] = df[col].astype("datetime64[ms]")

        # 2. Build Arrow schema with timestamp[ms] explicitly
        fields = []
        for col in df.columns:
            dtype = df[col].dtype

            if pd.api.types.is_integer_dtype(dtype):
                arrow_type = pa.int64()
            elif pd.api.types.is_float_dtype(dtype):
                arrow_type = pa.float64()
            elif pd.api.types.is_bool_dtype(dtype):
                arrow_type = pa.bool_()
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                arrow_type = pa.timestamp("ms")
            else:
                arrow_type = pa.string()

            fields.append(pa.field(col, arrow_type))

        schema = pa.schema(fields)

        # 3. Convert and write with explicit schema
        table = pa.Table.from_pandas(df, schema=schema)
        pq.write_table(table, batch_path)

        print(f"✅ Wrote batch to {batch_path}")
    except Exception as e:
        print("❌ Write permit batch Error: ", e)


def write_batch_to_gcs(df: pd.DataFrame, bucket_name: str, gcs_prefix: str):
    try:
        local_path = f"/tmp/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        
        # Downcast and Arrow conversion logic (avoid Spark datetime issue)
        df = df.copy()

        for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
            df[col] = df[col].astype("datetime64[ms]")

        # Build Arrow schema with timestamp[ms] explicitly
        fields = []
        for col in df.columns:
            dtype = df[col].dtype

            if pd.api.types.is_integer_dtype(dtype):
                arrow_type = pa.int64()
            elif pd.api.types.is_float_dtype(dtype):
                arrow_type = pa.float64()
            elif pd.api.types.is_bool_dtype(dtype):
                arrow_type = pa.bool_()
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                arrow_type = pa.timestamp("ms")
            else:
                arrow_type = pa.string()

            fields.append(pa.field(col, arrow_type))

        schema = pa.schema(fields)

        # Write locally to temp
        table = pa.Table.from_pandas(df, schema=schema)
        pq.write_table(table, local_path)

        # Upload to GCS (uses adc must grant sa permissions)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"{gcs_prefix}/{Path(local_path).name}")
        blob.upload_from_filename(local_path)

        print(f"✅ Uploaded batch to gs://{bucket_name}/{gcs_prefix}/")
    except Exception as e:
        print("❌ Error writing to GCS:", e)




async def get_record_list_csv(start_date: str, end_date: str):
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        try:
            await page.goto(BASE_URL, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle")
            await page.get_by_text("Search Additional Criteria").click()
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(1000)  # Give the JS time to finish

            
            start_date_input = page.locator("#ctl00_PlaceHolderMain_generalSearchForm_txtGSStartDate")
            await start_date_input.click()
            await start_date_input.fill("")
            await start_date_input.type(start_date)
            
            end_date_input = page.locator("#ctl00_PlaceHolderMain_generalSearchForm_txtGSEndDate")
            await end_date_input.click()
            await end_date_input.fill("")
            await end_date_input.type(end_date)
            

            res_com_type = await page.get_by_label("Residential or Commercial:").select_option(label="Residential")
            permit_type = await page.get_by_label("Permit Type:").nth(1).select_option(label="Solar Array")


            await page.locator("#ctl00_PlaceHolderMain_btnNewSearch").click()
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(5000)  # Give the JS time to finish


            async with page.expect_download() as download_info:
                await page.get_by_text("Download results").click()
            download = await download_info.value

            # Save the downloaded file
            target_path = DOWNLOAD_DIR / f"permit_records_{start_date}_to_{end_date}.csv"
            await download.save_as(str(target_path))
            print(f"✅ Downloaded to {target_path}")
            return target_path

        except Exception as e:
            print(f"❌ Error retrieving record list: {e}")
            return None

CONCURRENCY = 10  # You can tune this depending on your system
BATCH_SIZE = 50   # Number of permits per outer batch

async def scrape_batch(playwright, permits):
    browser = await playwright.chromium.launch(headless=True)
    # browser = await playwright.chromium.launch(headless=False, slow_mo=50)
    # context = await browser.new_context()
    results = []

    sem = asyncio.Semaphore(CONCURRENCY)

    async def worker(permit_number):
        async with sem:
            context = await browser.new_context()
            page = await context.new_page()
            result = await get_license_from_permit(page, permit_number)
            await page.close()
            return result

    tasks = [asyncio.create_task(worker(p)) for p in permits]
    results = await asyncio.gather(*tasks)
    await browser.close()
    return results



async def run_all(permits: pd.DataFrame, resume_file="./data/result_partial.csv"):
    all_results = []

    seen_permits = set()

    try:
        partial_df = pd.read_csv(resume_file)
        seen_permits = set(partial_df["Permit Number"].values)
        all_results.extend(zip(partial_df["Permit Number"], partial_df["Installer Number"]))
        print(f"✅ Loaded {len(seen_permits)} already processed permits.")
    except FileNotFoundError:
        pass


    async with async_playwright() as playwright:
        for i in range(0, len(permits), BATCH_SIZE):
            batch = permits.iloc[i:i+BATCH_SIZE]
            batch = batch[~batch["Permit Number"].isin(seen_permits)]

            if batch.empty:
                continue

            print(f"🔄 Processing batch {i // BATCH_SIZE + 1}")

            try:
                batch_results = await scrape_batch(playwright, batch["Permit Number"])
                results_df = pd.DataFrame(batch_results, columns=["Permit Number", "Installer Number"])
                batch_with_installers = batch.merge(results_df, on="Permit Number", how="left")

                all_results.extend(batch_with_installers.values)

                # Write partial results to CSV
                # df_partial = pd.DataFrame(batch_results, columns=["Permit Number", "Installer Number"])
                # df_partial.to_csv(resume_file, mode='a', index=False, header=not seen_permits)
                
                
                # Write partial results to postgres
                # insert_batch_to_postgres(batch_with_installers)

                # Write partial results to iceberg
                # write_batch_to_iceberg(batch_with_installers)

                # Write partial results to mounted data dir for iceberg processing
                write_batch_to_storage(batch_with_installers)

                # Write partial results to GCS
                # write_batch_to_gcs(batch_with_installers, GCS_BUCKET, GCS_PREFIX)

                print(f"💾 Saved batch {i // BATCH_SIZE + 1}")
            except Exception as e:
                print(f"🔥 Error during batch {i // BATCH_SIZE + 1}: {e}")
                break
        
            
    print(f"size of all results before returning {len(all_results)}")
    return all_results



async def main():
    start_date, end_date = get_previous_month_date_range()
    target_path = await get_record_list_csv(start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y"))
    if not target_path or not Path(target_path).exists():
        print("❌ Failed to download record list CSV")
        return

    print(f"📄 Using downloaded file: {target_path}")
    df = pd.read_csv(target_path, parse_dates=["Date"])
    df.drop(columns=["Unnamed: 7"], inplace=True)
    # df = df.iloc[-75:, :]  # You can remove this slice once done testing
    print(f"current size of df pre function: {df.shape}")

    
    results = await run_all(df)
    print(results)

    missing_count = sum(1 for row in results if row[-1] is None)
    total_count = len(results)
    print(f"\n🔢 License numbers not found: {missing_count} / {total_count} ({missing_count / total_count:.1%} missing)")

    print("Final results generated!")

if __name__ == "__main__":
    asyncio.run(main())