#  Data Quality Pipeline 

##  Project Overview

This project is an end-to-end **Data Quality Pipeline** built using **Apache Spark**, **Apache Airflow**, **MongoDB**, and **Docker**.

The pipeline validates trading datasets (Equity, Bond, and Commodity), separates valid and invalid records, stores the results in MongoDB, and uses Airflow to orchestrate and schedule the complete workflow.

---

##  Architecture

```text
                    Apache Airflow
                          │
                   Trading Pipeline DAG
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
      Equity Job      Bond Job      Commodity Job
          │               │               │
          ▼               ▼               ▼
             Apache Spark Processing
                          │
          ┌───────────────┼───────────────┐
          ▼                               ▼
     Valid Records                  Invalid Records
          │                               │
          └───────────────┬───────────────┘
                          ▼
                       MongoDB
```

---

##  Technologies Used

* Apache Spark
* Apache Airflow
* MongoDB
* PostgreSQL (Airflow Metadata Database)
* Docker & Docker Compose
* Python
* PySpark

---

##  How to Run

### 1. Clone the repository

```bash
git clone <https://github.com/dhruvi-core/data-quality-pipeline.git>
cd data-quality-pipeline
```

### 2. Start Docker containers

```bash
docker compose up -d
```

### 3. Open Airflow

```
http://localhost:8081
```

### 4. Trigger the DAG

Open **trading_pipeline** from the Airflow UI and trigger it manually or let it run based on the configured schedule.