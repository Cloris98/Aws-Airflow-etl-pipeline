# AWS-Airflow Cloud-Based ETL Pipeline

This project builds a scalable ETL pipeline using AWS services, orchestrated with Apache Airflow running in Docker.

---

## 🧱 Architecture

- **Data Source:** Kaggle dataset uploaded to S3 (`rawdata-aws-airflow`)
- **ETL Tool:** AWS Glue Jobs (triggered via Airflow)
- **Query & Transformation Planning:** AWS Athena
- **Orchestration:** Apache Airflow running in Docker (local)
- **Storage:**
  - Raw Data: `s3://rawdata-aws-airflow`
  - Transformed Data: `s3://etldata-aws-airflow`
  - Glue Scripts: `s3://glue-script-aws-airflow/scripts`
  - Athena Query Results: `s3://athena-results-airflow-jingwei`

---

## 🗂️ ETL Stages

| Table                     | Notes                                   |
|---------------------------|------------------------------------------|
| `customer_table`          | Handles gender recoding, address merging |
| `transaction_table`       | Drops time columns, adds txn count       |
| `account_table`           |Includes account numbers and summaries    |
| `merchant_metrics_table`  | Metrics on merchant risk & customer behavior|

---

## 🚀 Running the Project

### 1. Start Airflow with Docker

```bash
docker-compose up -d
```

### 2. Open Airflow UI

Go to [http://localhost:8080](http://localhost:8080)  
Enable and trigger the desired DAG (e.g., `customer_etl_dag`, `etl_dag`)

## 🔐 IAM Role Setup

To run this ETL pipeline successfully, your AWS Glue job must be assigned an **IAM role** with the necessary permissions.

> ✅ **Make sure your IAM role has permissions to:**

- Run AWS Glue jobs  
- **Read from** your raw S3 bucket (e.g., `rawdata-aws-airflow`)  
- **Write to** your transformed S3 bucket (e.g., `etldata-aws-airflow`)  
- Access Glue scripts and Athena query results if needed  

---

### 🛠️ Example IAM Permissions

Attach policies like:

- `AmazonS3FullAccess` *(or more narrowly scoped custom policy)*
- `AWSGlueServiceRole`
- *Optionally*: `AmazonAthenaFullAccess` (if using Athena)

> **Tip:** If using a custom role like `AWSGlueServiceRole-airflow-xxx`, double check that it has all needed S3 and Glue permissions.


