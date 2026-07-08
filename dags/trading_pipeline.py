from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from datetime import datetime, timedelta

default_args = {
    "owner": "admin",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="trading_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data-engineering", "spark"],
) as dag:


    # Start Task
    
    start = EmptyOperator(
        task_id="start"
    )

    
    # Run Equity,bond,commodity Spark Job
   
    equity = BashOperator(
        task_id="run_equity",
        bash_command="docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/equity.py"
    )

    bond = BashOperator(
    task_id="run_bond",
    bash_command="docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/bond.py"
    )

    commodity = BashOperator(
        task_id="run_commodity",
        bash_command="docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/commodity.py"
    )

    
    # End Task
    
    end = EmptyOperator(
        task_id="end"
    )

    
    # Task Dependencies
    
    start >> [equity, bond, commodity] >> end