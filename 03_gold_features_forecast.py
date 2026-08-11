# =========================
# Widgets
# =========================
dbutils.widgets.text("container", "stockproject")
dbutils.widgets.text("storage_account", "ststockforecastdev01")
dbutils.widgets.text("max_loaded_date", "")

container = dbutils.widgets.get("container").strip()
storage_account = dbutils.widgets.get("storage_account").strip()
max_loaded_date = dbutils.widgets.get("max_loaded_date").strip()

# =========================
# Imports
# =========================
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
from prophet import Prophet
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, LongType
)

# =========================
# Helper: write single CSV file
# =========================
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

# =========================
# Paths
# =========================
silver_path   = f"abfss://{container}@{storage_account}.dfs.core.windows.net/silver/hdfc_clean"
features_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/gold/hdfc_features"
forecast_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/gold/hdfc_forecast"
metrics_path  = f"abfss://{container}@{storage_account}.dfs.core.windows.net/gold/hdfc_model_metrics"
watermark_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/metadata/hdfc_watermark.json"

features_csv_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/readable/gold/hdfc_features.csv"
forecast_csv_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/readable/gold/hdfc_forecast.csv"
metrics_csv_path  = f"abfss://{container}@{storage_account}.dfs.core.windows.net/readable/gold/hdfc_model_metrics.csv"

# LSTM model paths:
#   - local temp file inside driver VM
#   - ADLS path where the model is stored permanently
lstm_local_path = "/tmp/hdfc_lstm_base.keras"
lstm_adls_path  = f"abfss://{container}@{storage_account}.dfs.core.windows.net/artifacts/models/lstm/hdfc_lstm_base.keras"

print(f"Silver path:   {silver_path}")
print(f"Features path: {features_path}")
print(f"Forecast path: {forecast_path}")
print(f"Metrics path:  {metrics_path}")
print(f"LSTM local path: {lstm_local_path}")
print(f"LSTM ADLS path:  {lstm_adls_path}")

# =========================
# Read silver layer
# =========================
spark_df = spark.read.parquet(silver_path)
if spark_df.rdd.isEmpty():
    raise Exception("Silver layer is empty")

pdf = spark_df.toPandas().copy()

# Validate columns
required_cols = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "ticker"]
missing_cols = [c for c in required_cols if c not in pdf.columns]
if missing_cols:
    raise Exception(f"Missing required columns in silver data: {missing_cols}")

# Type conversions
pdf["Date"] = pd.to_datetime(pdf["Date"], errors="coerce")
pdf["Close"] = pd.to_numeric(pdf["Close"], errors="coerce")
pdf["Open"]  = pd.to_numeric(pdf["Open"],  errors="coerce")
pdf["High"]  = pd.to_numeric(pdf["High"],  errors="coerce")
pdf["Low"]   = pd.to_numeric(pdf["Low"],   errors="coerce")
pdf["Adj Close"] = pd.to_numeric(pdf["Adj Close"], errors="coerce")
pdf["Volume"]    = pd.to_numeric(pdf["Volume"],    errors="coerce")

pdf = pdf.dropna(subset=["Date", "Close"]).sort_values("Date").reset_index(drop=True)
if pdf.empty:
    raise Exception("Silver layer has no valid Date/Close rows after cleaning")

max_date = pdf["Date"].max()
latest_close = float(pdf["Close"].iloc[-1])

# Restrict to last ~10 years
window_start = max_date - pd.Timedelta(days=3650)
pdf = pdf[pdf["Date"] >= window_start].copy().reset_index(drop=True)

base_rows = len(pdf)
print(f"Rows before feature engineering: {base_rows}")
print(f"Max training date: {max_date.date()}")
print(f"Latest close used for fallback: {latest_close}")

# =========================
# Feature engineering
# =========================
pdf["lag_1"] = pdf["Close"].shift(1)
pdf["lag_2"] = pdf["Close"].shift(2)
pdf["lag_3"] = pdf["Close"].shift(3)
pdf["ma_5"]  = pdf["Close"].rolling(5).mean()
pdf["ma_10"] = pdf["Close"].rolling(10).mean()

features_pdf = pdf.dropna().copy().reset_index(drop=True)
print(f"Rows after feature engineering: {len(features_pdf)}")

# Save features to gold
features_schema = StructType([
    StructField("Date", TimestampType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Adj Close", DoubleType(), True),
    StructField("Volume", LongType(), True),
    StructField("ticker", StringType(), True),
    StructField("lag_1", DoubleType(), True),
    StructField("lag_2", DoubleType(), True),
    StructField("lag_3", DoubleType(), True),
    StructField("ma_5", DoubleType(), True),
    StructField("ma_10", DoubleType(), True)
])

if not features_pdf.empty:
    features_spark_df = spark.createDataFrame(features_pdf)
else:
    features_spark_df = spark.createDataFrame([], schema=features_schema)

features_spark_df.write.mode("overwrite").parquet(features_path)
write_single_csv(features_spark_df, features_csv_path)

# =========================
# Default forecast values and statuses
# =========================
prophet_next = latest_close
lstm_next = latest_close
prophet_status = "not_run"
lstm_status = "not_run"
model_used = "fallback_latest_close"
prophet_error = ""
lstm_error = ""

# =========================
# Prophet model
# =========================
if len(pdf) >= 20:
    try:
        print("Running Prophet model")
        prophet_df = pdf[["Date", "Close"]].rename(columns={"Date": "ds", "Close": "y"}).copy()
        prophet_df["ds"] = pd.to_datetime(prophet_df["ds"], errors="coerce")
        prophet_df["y"] = pd.to_numeric(prophet_df["y"], errors="coerce")
        prophet_df = prophet_df.dropna().sort_values("ds")

        if len(prophet_df) < 20:
            raise Exception("Not enough valid rows after Prophet input cleaning")

        prophet_model = Prophet(
            daily_seasonality=False,
            weekly_seasonality=True,
            yearly_seasonality=False
        )
        prophet_model.fit(prophet_df)
        future = prophet_model.make_future_dataframe(periods=1)
        prophet_forecast = prophet_model.predict(future)
        prophet_next = float(prophet_forecast["yhat"].iloc[-1])
        prophet_status = "success"
        model_used = "prophet"
        print(f"Prophet forecast success: {prophet_next}")
    except Exception as e:
        prophet_status = "fallback"
        prophet_error = str(e)
        print(f"Prophet fallback used: {prophet_error}")
else:
    prophet_status = "insufficient_rows"
    prophet_error = f"Prophet needs >= 20 rows, found {len(pdf)}"
    print(prophet_error)

# =========================
# LSTM model - incremental using /tmp + ADLS
# =========================
if len(features_pdf) > 10:
    try:
        print("Running LSTM model (incremental mode)")

        close_values = features_pdf[["Close"]].values

        # Scale closes on latest window
        scaler = MinMaxScaler()
        scaled = scaler.fit_transform(close_values)

        seq_len = 10
        if len(scaled) < seq_len:
            raise Exception(f"Need at least {seq_len} rows for a sequence, found {len(scaled)}")

        # Try to copy model from ADLS to local /tmp and load it
        lstm_model_loaded = False
        try:
            dbutils.fs.cp(lstm_adls_path, f"file:{lstm_local_path}", True)
            print(f"Copied LSTM model from ADLS to local: {lstm_adls_path} -> {lstm_local_path}")
            model = load_model(lstm_local_path)
            print("Loaded existing LSTM model from local /tmp")
            lstm_model_loaded = True
        except Exception as e_load:
            print(f"Could not load LSTM model from ADLS/local (first run or missing). Error: {e_load}")
            lstm_model_loaded = False

        if not lstm_model_loaded:
            # Base training on full 10-year window
            print("No existing LSTM model found. Training base model.")
            X, y = [], []
            for i in range(seq_len, len(scaled)):
                X.append(scaled[i-seq_len:i, 0])
                y.append(scaled[i, 0])

            X = np.array(X)
            y = np.array(y)

            if len(X) == 0:
                raise Exception("LSTM sequence generation returned zero samples during base training")

            X = X.reshape((X.shape[0], X.shape[1], 1))

            model = Sequential()
            model.add(LSTM(50, input_shape=(X.shape[1], 1)))
            model.add(Dense(1))
            model.compile(optimizer="adam", loss="mse")
            model.fit(X, y, epochs=5, batch_size=16, verbose=0)

            # Save base model to /tmp and copy to ADLS
            try:
                model.save(lstm_local_path)
                print(f"Saved base LSTM model to local: {lstm_local_path}")
                dbutils.fs.cp(f"file:{lstm_local_path}", lstm_adls_path, True)
                print(f"Copied base LSTM model from local to ADLS: {lstm_adls_path}")
            except Exception as e_save:
                print(f"ERROR saving/copying LSTM model: {e_save}")

        # Predict next day using latest 10 closes
        last_seq = scaled[-seq_len:].reshape((1, seq_len, 1))
        lstm_pred_scaled = model.predict(last_seq, verbose=0)
        lstm_next = float(scaler.inverse_transform(lstm_pred_scaled)[0][0])

        lstm_status = "success_incremental"
        if model_used == "prophet":
            model_used = "prophet_lstm"
        else:
            model_used = "lstm_incremental"

        print(f"LSTM incremental forecast success: {lstm_next}")

    except Exception as e:
        lstm_status = "fallback"
        lstm_error = str(e)
        print(f"LSTM fallback used: {lstm_error}")
else:
    lstm_status = "insufficient_rows"
    lstm_error = f"LSTM needs > 10 feature rows, found {len(features_pdf)}"
    print(lstm_error)

# =========================
# Helper: get next trading day (skip Saturday/Sunday)
# =========================
def get_next_trading_day(d):
    next_date = d + pd.Timedelta(days=1)
    while next_date.weekday() >= 5:
        next_date += pd.Timedelta(days=1)
    return next_date

# =========================
# Forecast table (append history)
# =========================
ticker_val = str(pdf["ticker"].iloc[-1]) if "ticker" in pdf.columns and not pdf["ticker"].isna().all() else "HDFCBANK.NS"

# Use next trading day instead of max_date + 1 calendar day
forecast_dt = get_next_trading_day(max_date)
forecast_date_str = str(forecast_dt.date())

new_forecast_output = pd.DataFrame([{
    "forecast_date": forecast_date_str,
    "ticker": ticker_val,
    "prophet_forecast": float(prophet_next),
    "lstm_forecast": float(lstm_next),
    "latest_close_fallback": float(latest_close),
    "model_used": model_used,
    "run_timestamp": str(datetime.now())
}])

# NOTE: Your original notebook 3 content was cut off at this point in the
# message you pasted. Everything above this line is reproduced exactly as
# provided. You'll need to add back whatever came after this point in your
# actual Databricks notebook (e.g. writing new_forecast_output to the
# forecast_path table, writing model metrics to metrics_path, updating the
# watermark file, and the final dbutils.notebook.exit(...) call).
