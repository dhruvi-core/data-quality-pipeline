import requests
from pymongo import MongoClient
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy.orm import Session

from database import engine, get_db
from models import Base, ValidationRule
from schemas import ValidationRuleCreate


Base.metadata.create_all(bind=engine)


app = FastAPI(
    title="Data Quality Pipeline API"
)

mongo_client = MongoClient(
    "mongodb://trading-mongodb:27017"
)

mongo_db = mongo_client["trading"]

# Airflow Configuration

AIRFLOW_URL = "http://airflow-webserver:8080"
AIRFLOW_DAG_ID = "data_quality_pipeline"

AIRFLOW_USERNAME = "admin"
AIRFLOW_PASSWORD = "admin"


# Allow React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def home():

    return {
        "message": "Data Quality Pipeline API is running"
    }


@app.post("/rules")
def create_rule(
    rule: ValidationRuleCreate,
    db: Session = Depends(get_db)
):

    new_rule = ValidationRule(
        dataset=rule.dataset,
        column_name=rule.column_name,
        column_type=rule.column_type,
        operator=rule.operator,
        value=rule.value
    )

    db.add(new_rule)

    db.commit()

    db.refresh(new_rule)

    return {
        "message": "Validation rule created",
        "rule": {
            "id": new_rule.id,
            "dataset": new_rule.dataset,
            "column_name": new_rule.column_name,
            "column_type": new_rule.column_type,
            "operator": new_rule.operator,
            "value": new_rule.value
        }
    }


@app.get("/rules")
def get_rules(
    db: Session = Depends(get_db)
):

    rules = db.query(
        ValidationRule
    ).all()

    return rules

@app.delete("/rules/{rule_id}")
def delete_rule(
    rule_id: int,
    db: Session = Depends(get_db)
):

    rule = db.query(ValidationRule).filter(
        ValidationRule.id == rule_id
    ).first()

    if not rule:
        return {
            "message": "Rule not found"
        }

    db.delete(rule)
    db.commit()

    return {
        "message": "Validation rule deleted successfully"
    }

@app.get("/quality-summary")
def get_quality_summary():

    datasets = [
        "equity",
        "bond",
        "commodity"
    ]

    dataset_results = []

    total_good = 0
    total_bad = 0

    for dataset in datasets:

        good_collection = mongo_db[
            f"{dataset}_good"
        ]

        bad_collection = mongo_db[
            f"{dataset}_bad"
        ]

        good_count = good_collection.count_documents({})
        bad_count = bad_collection.count_documents({})

        total_count = good_count + bad_count

        if total_count > 0:
            quality_score = round(
                (good_count / total_count) * 100,
                2
            )
        else:
            quality_score = 0

        dataset_results.append({
            "dataset": dataset,
            "total_records": total_count,
            "good_records": good_count,
            "bad_records": bad_count,
            "quality_score": quality_score
        })

        total_good += good_count
        total_bad += bad_count

    overall_total = total_good + total_bad

    if overall_total > 0:
        overall_score = round(
            (total_good / overall_total) * 100,
            2
        )
    else:
        overall_score = 0

    return {
        "overall": {
            "total_records": overall_total,
            "good_records": total_good,
            "bad_records": total_bad,
            "quality_score": overall_score
        },
        "datasets": dataset_results
    }

@app.post("/run-pipeline")
def run_pipeline():

    url = (
        f"{AIRFLOW_URL}/api/v1/dags/"
        f"{AIRFLOW_DAG_ID}/dagRuns"
    )

    try:

        response = requests.post(
            url,
            json={
                "conf": {}
            },
            auth=(
                AIRFLOW_USERNAME,
                AIRFLOW_PASSWORD
            ),
            timeout=10
        )

        response.raise_for_status()

        dag_run = response.json()

        return {
            "message": "Pipeline started",
            "dag_run_id": dag_run["dag_run_id"],
            "state": dag_run["state"]
        }

    except requests.RequestException as error:

        raise HTTPException(
            status_code=500,
            detail=f"Failed to start pipeline: {str(error)}"
        )
    
@app.get("/pipeline-status/{dag_run_id}")
def get_pipeline_status(
    dag_run_id: str
):

    url = (
        f"{AIRFLOW_URL}/api/v1/dags/"
        f"{AIRFLOW_DAG_ID}/dagRuns/"
        f"{dag_run_id}"
    )

    try:

        response = requests.get(
            url,
            auth=(
                AIRFLOW_USERNAME,
                AIRFLOW_PASSWORD
            ),
            timeout=10
        )

        response.raise_for_status()

        dag_run = response.json()

        return {
            "dag_run_id": dag_run["dag_run_id"],
            "state": dag_run["state"]
        }

    except requests.RequestException as error:

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to get pipeline status: "
                f"{str(error)}"
            )
        )