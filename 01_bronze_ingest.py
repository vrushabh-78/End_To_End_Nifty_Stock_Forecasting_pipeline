dbutils.widgets.text("container", "stockproject")
dbutils.widgets.text("storage_account", "ststockforecastdev01")

container = dbutils.widgets.get("container").strip()
storage_account = dbutils.widgets.get("storage_account").strip()

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def write_single_csv(spark_df, final_csv_path):
    temp_path = final_csv_path + "_tmp"
    dbutils.fs.rm(temp_path, True)
    dbutils.fs.rm(final_csv_path, True)
    spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
    files = dbutils.fs.ls(temp_path)
    part_file = [f.path for f in files if f.path.endswith(".csv")][0]
    dbutils.fs.cp(part_file, final_csv_path)
    dbutils.fs.rm(temp_path, True)
    print(f"Readable CSV written at: {final_csv_path}")

watermark_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/metadata/hdfc_watermark.json"
bronze_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/hdfc"
bronze_csv_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/readable/bronze/hdfc.csv"

today = datetime.today().date()

print(f"Watermark path: {watermark_path}")
print(f"Bronze path: {bronze_path}")
print(f"Bronze CSV path: {bronze_csv_path}")

try:
    wm_df = spark.read.option("multiline", "true").json(watermark_path)
    wm_rows = wm_df.collect()
    if len(wm_rows) > 0:
        wm = wm_rows[0].asDict()
        last_loaded_date = wm.get("last_loaded_date", "")
    else:
        last_loaded_date = ""
except Exception as e:
    print(f"Watermark read failed: {str(e)}")
    last_loaded_date = ""

bronze_exists_and_has_data = False
try:
    existing_bronze = spark.read.parquet(bronze_path)
    if not existing_bronze.rdd.isEmpty():
        bronze_exists_and_has_data = True
except Exception as e:
    print(f"Bronze read check failed (likely missing): {str(e)}")
    bronze_exists_and_has_data = False

print(f"Watermark last_loaded_date: {last_loaded_date}")
print(f"Bronze has existing data: {bronze_exists_and_has_data}")

if not bronze_exists_and_has_data:
    print("Bronze is missing or empty. Forcing full 10-year reload regardless of watermark.")
    last_loaded_date = ""

if last_loaded_date in [None, ""]:
    start_date = (today - timedelta(days=3650)).strftime("%Y-%m-%d")
    print("Running FULL historical load (10 years)")
else:
    start_date = (datetime.strptime(last_loaded_date, "%Y-%m-%d").date() + timedelta(days=1)).strftime("%Y-%m-%d")
    print("Running INCREMENTAL load")

end_date = (today + timedelta(days=1)).strftime("%Y-%m-%d")

print(f"Start date: {start_date}")
print(f"End date: {end_date}")

df = yf.download(
    "HDFCBANK.NS",
    start=start_date,
    end=end_date,
    interval="1d",
    progress=False,
    auto_adjust=False,
    multi_level_index=False
)

print("Raw columns before flatten:", df.columns)

if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.get_level_values(0)

print("Columns after flatten:", df.columns.tolist())
print(f"Rows fetched from yfinance: {len(df)}")

if df.empty:
    max_loaded_date = last_loaded_date if last_loaded_date else ""
    print("No new data returned from yfinance")
    dbutils.notebook.exit(max_loaded_date)

df.index.name = "Date"
df = df.reset_index()

print("Columns after reset_index:", df.columns.tolist())

rename_map = {}
for c in df.columns:
    c_str = str(c).strip()
    if c_str.lower() in ["date", "index", "datetime"]:
        rename_map[c] = "Date"
    elif c_str.lower() == "adj close":
        rename_map[c] = "Adj Close"
    elif c_str.lower() == "open":
        rename_map[c] = "Open"
    elif c_str.lower() == "high":
        rename_map[c] = "High"
    elif c_str.lower() == "low":
        rename_map[c] = "Low"
    elif c_str.lower() == "close":
        rename_map[c] = "Close"
    elif c_str.lower() == "volume":
        rename_map[c] = "Volume"

df = df.rename(columns=rename_map)

print("Columns after rename:", df.columns.tolist())

required_cols = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
for c in required_cols:
    if c not in df.columns:
        if c == "Adj Close" and "Close" in df.columns:
            df[c] = df["Close"]
        else:
            raise Exception(f"Missing required column: {c}. Available columns: {df.columns.tolist()}")

df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df.dropna(subset=["Date"])

df = df[required_cols]
df["ticker"] = "HDFCBANK.NS"

print("Final bronze columns:", df.columns.tolist())
print(f"Final row count: {len(df)}")
print(df.head())
print(df.tail())

if not df.empty:
    spark_df = spark.createDataFrame(df)

    if bronze_exists_and_has_data and last_loaded_date not in [None, ""]:
        spark_df.write.mode("append").parquet(bronze_path)
        print("Appended to existing bronze data")
    else:
        spark_df.write.mode("overwrite").parquet(bronze_path)
        print("Overwrote bronze data with full historical load")

    full_bronze_df = spark.read.parquet(bronze_path)
    write_single_csv(full_bronze_df, bronze_csv_path)

    max_loaded_date = str(pd.to_datetime(df["Date"]).max().date())
    total_rows = full_bronze_df.count()
    print(f"Bronze parquet and CSV written. Max loaded date: {max_loaded_date}")
    print(f"Total rows now in bronze: {total_rows}")
else:
    max_loaded_date = last_loaded_date if last_loaded_date else ""
    print("No valid rows after cleaning")

dbutils.notebook.exit(max_loaded_date)
