# End-to-End Nifty Stock Forecasting Pipeline

![Python](https://img.shields.io/badge/Python-3.x-blue) ![Airflow](https://img.shields.io/badge/Airflow-workflow-orange) ![Azure](https://img.shields.io/badge/Azure-Databricks%20%7C%20ADF%20%7C%20ADLS-0078D4) ![LSTM](https://img.shields.io/badge/Model-LSTM-green) ![Prophet](https://img.shields.io/badge/Model-Prophet-lightgrey)

Automated end-to-end stock forecasting pipeline (currently configured for **HDFCBANK.NS**, extendable to any Nifty stock) using Apache Airflow, Azure Data Factory, Azure Databricks, and Azure Data Lake Storage Gen2, with LSTM and Prophet models following a Medallion Architecture.

## 📌 Project Overview
This project implements an automated cloud-based stock data engineering and forecasting pipeline.

The pipeline collects historical stock market data for HDFC Bank (HDFCBANK.NS), processes it through Bronze, Silver, and Gold layers using PySpark and Databricks, and generates stock price forecasts using LSTM and Prophet models.

The workflow is orchestrated using Apache Airflow and Azure Data Factory, while Azure Data Lake Storage Gen2 is used for data storage. The architecture is designed to be stock-agnostic — any Nifty-listed ticker can be plugged in with minimal configuration changes.

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
```
