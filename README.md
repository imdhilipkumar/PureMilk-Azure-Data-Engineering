# PureMilk – End-to-End Azure Data Engineering Project

## 📌 Project Overview

PureMilk is an end-to-end Azure Data Engineering project designed to process Smart Dairy Milk Silo sensor data. The pipeline ingests streaming data, validates and transforms records, separates valid and rejected data, and loads processed records into Azure SQL Database.

The project demonstrates data ingestion, ETL/ELT processing, data quality validation, incremental data loading, watermark management, and pipeline orchestration using Azure services.

## 🎯 Project Objectives

- Ingest milk silo sensor data in JSON format.
- Process incoming data through an automated data pipeline.
- Validate sensor readings and identify invalid records.
- Transform and organize data into Bronze, Silver, and Gold layers.
- Load validated data into Azure SQL Database.
- Implement incremental processing using watermark-based tracking.
- Maintain rejected records for data quality analysis.

## 🏗️ Architecture

**Data Generator → Azure Event Hubs → Event Consumer → Azure Blob Storage → Azure Data Factory → Azure SQL Database**

The pipeline processes data through the following stages:

1. **Data Ingestion:** Generate milk silo sensor data and stream events through Azure Event Hubs.
2. **Raw Storage:** Store incoming JSON files in Azure Blob Storage.
3. **Bronze Layer:** Ingest and prepare raw data for transformation.
4. **Silver Layer:** Clean data, validate sensor readings, remove duplicates, and separate rejected records.
5. **Gold Layer:** Prepare validated data for downstream consumption.
6. **Azure SQL Database:** Load the final processed data.
7. **Incremental Processing:** Track processed data using a watermark table.
8. **Orchestration:** Use Azure Data Factory pipelines and triggers to coordinate processing.

## 🛠️ Technologies Used

- Microsoft Azure
- Azure Data Factory (ADF)
- Azure Event Hubs
- Azure Blob Storage
- Azure SQL Database
- Python
- SQL
- Data Flows
- JSON
- Parquet
- ETL/ELT
- Incremental Data Loading
- Data Quality Validation

## 🧪 Data Quality Checks

The pipeline applies validation rules to milk silo sensor records, including:

- Sample ID and silo ID validation.
- Handling missing values and invalid sensor readings.
- Temperature, pH, acidity, fat, SNF, and density range checks.
- Duplicate record handling.
- Separation of valid and rejected records for further analysis.

## ⚙️ Key Project Components

| Component | Purpose |
|---|---|
| Python Data Generator | Generates milk silo sensor data |
| Azure Event Hubs | Ingests streaming events |
| Event Consumer | Writes consumed data to storage |
| Azure Blob Storage | Stores incoming JSON files |
| Azure Data Factory | Orchestrates pipeline execution and transformations |
| Azure SQL Database | Stores final processed data |
| Watermark Table | Tracks incremental processing progress |

## 📊 Project Outcomes

- Built an end-to-end data ingestion and processing workflow.
- Implemented data validation and rejected-record handling.
- Organized processing into Bronze, Silver, and Gold layers.
- Implemented watermark-based incremental processing.
- Integrated Azure Data Factory with Azure SQL Database.

## 📸 Screenshots and Evidence

Add screenshots of the following as you make them available:

- Overall pipeline architecture.
- Azure Data Factory pipeline execution.
- Data Flow transformations.
- Bronze, Silver, and Gold storage layers.
- Final SQL table and watermark table.
- Successful incremental pipeline runs.

## 👨‍💻 Author

**Dhilip Kumar**

Azure Data Engineering | Azure Data Factory | Python | SQL | PySpark

[LinkedIn](https://www.linkedin.com/in/dhilipkumar20) | [GitHub](https://github.com/imdhilipkumar)

---

*This project was developed for hands-on learning and portfolio demonstration of Azure Data Engineering concepts.*
