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


🔄 Pipeline Workflow
Apache Airflow triggers the Azure Data Factory pipeline.
Azure Data Factory executes the pl_hdfc_master pipeline.
Databricks fetches HDFC stock data.
Raw data is stored in the Bronze layer of ADLS Gen2.
Databricks cleans and transforms the Bronze data into the Silver layer.
Feature engineering is performed on the processed data.
Analytics-ready data is stored in the Gold layer.
LSTM and Prophet models are used for stock price forecasting.
The final forecast output is stored in ADLS Gen2.
🥉 Bronze Layer

Contains raw stock market data collected from the source.

ADLS Gen2
└── bronze/
🥈 Silver Layer

Contains cleaned, standardized, and validated stock data.

ADLS Gen2
└── silver/
🥇 Gold Layer

Contains feature-engineered and analytics-ready data along with forecasting results.

ADLS Gen2
└── gold/
🤖 Machine Learning Models
LSTM

Long Short-Term Memory neural network used for time-series stock price forecasting.

Prophet

Time-series forecasting model used to generate stock price predictions.

🛠️ Technology Stack
Technology	Purpose
Python	Programming
Apache Airflow	Workflow orchestration
Azure Data Factory	Data pipeline orchestration
Azure Databricks	Data processing and ML
PySpark	Distributed data processing
ADLS Gen2	Cloud data lake storage
LSTM	Stock forecasting
Prophet	Stock forecasting
Docker	Airflow environment

📂 Project Structure
End_To_End_Nifty_Stock_Forecasting_pipeline/
│
├── airflow/
│   └── dags/
│       └── trigger_hdfc_adf_pipeline.py
│
├── databricks/
│   ├── 01_fetch_hdfc_bronze.py
│   ├── 02_bronze_to_silver.py
│   └── 03_silver_to_gold_train_forecast.py
│
├── adf/
│   └── pl_hdfc_master.json
│
├── architecture/
│   └── architecture_diagram.png
│
├── screenshots/
│   ├── airflow_dag.png
│   ├── databricks_notebooks.png
│   ├── adls_storage.png
│   └── gold_forecast_output.png
│
├── docs/
│   ├── Project_Report.pdf
│   └── Project_Presentation.pptx
│
├── README.md
├── .gitignore
└── LICENSE

📚 Documentation
Project Report
Project Presentation

🎯 Key Features
End-to-end automated data pipeline
Cloud-based data lake architecture
Medallion Architecture implementation
Automated workflow orchestration
Distributed data processing using PySpark
Feature engineering for time-series forecasting
LSTM and Prophet forecasting
Azure-based deployment


