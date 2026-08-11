dbutils.widgets.text("container", "stockproject")
dbutils.widgets.text("storage_account", "ststockforecastdev01")

container = dbutils.widgets.get("container").strip()
storage_account = dbutils.widgets.get("storage_account").strip()

from pyspark.sql.functions import col, to_date

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

bronze_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/hdfc"
silver_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/silver/hdfc_clean"
silver_csv_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/readable/silver/hdfc_clean.csv"

print(f"Bronze path: {bronze_path}")
print(f"Silver path: {silver_path}")
print(f"Silver CSV path: {silver_csv_path}")

df = spark.read.parquet(bronze_path)
df.printSchema()
print("Bronze columns:", df.columns)

required_cols = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "ticker"]
for c in required_cols:
    if c not in df.columns:
        raise Exception(f"Missing required bronze column: {c}")

clean_df = (
    df.withColumn("Date", to_date(col("Date")))
      .withColumn("Open", col("Open").cast("double"))
      .withColumn("High", col("High").cast("double"))
      .withColumn("Low", col("Low").cast("double"))
      .withColumn("Close", col("Close").cast("double"))
      .withColumn("Adj Close", col("Adj Close").cast("double"))
      .withColumn("Volume", col("Volume").cast("long"))
      .dropDuplicates(["Date", "ticker"])
      .orderBy("Date")
)

clean_df.write.mode("overwrite").parquet(silver_path)
write_single_csv(clean_df, silver_csv_path)

print("Silver parquet and CSV written successfully")
dbutils.notebook.exit("silver_done")
