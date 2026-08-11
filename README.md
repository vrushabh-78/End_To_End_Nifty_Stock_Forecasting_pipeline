# End-to-End Nifty Stock Forecasting Pipeline

Automated end-to-end HDFC stock forecasting pipeline using Apache Airflow, Azure Data Factory, Azure Databricks, and Azure Data Lake Storage Gen2, with LSTM and Prophet models following a Medallion Architecture.

## 📌 Project Overview

This project implements an automated cloud-based stock data engineering and forecasting pipeline.

The pipeline collects HDFC stock market data, processes it through Bronze, Silver, and Gold layers using PySpark and Databricks, and generates stock price forecasts using LSTM and Prophet models.

The workflow is orchestrated using Apache Airflow and Azure Data Factory, while Azure Data Lake Storage Gen2 is used for data storage.

## 🏗️ Architecture

```text
Yahoo Finance
      ↓
Apache Airflow
      ↓
Azure Data Factory
      ↓
Azure Databricks
      ↓
ADLS Gen2
      ↓
Bronze → Silver → Gold
      ↓
LSTM + Prophet
      ↓
HDFC Forecast Output
