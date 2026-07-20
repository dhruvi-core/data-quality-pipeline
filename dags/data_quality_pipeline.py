from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime


with DAG(

    dag_id="data_quality_pipeline",

    start_date=datetime(
        2026,
        1,
        1
    ),

    schedule=None,

    catchup=False,

    tags=[
        "data-quality"
    ],

) as dag:


    # ==========================================
    # Equity Validation
    # ==========================================

    validate_equity = BashOperator(

        task_id="validate_equity",

        bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        /opt/spark/jobs/equity.py
        """

    )


    # ==========================================
    # Bond Validation
    # ==========================================

    validate_bond = BashOperator(

        task_id="validate_bond",

        bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        /opt/spark/jobs/bond.py
        """

    )


    # ==========================================
    # Commodity Validation
    # ==========================================

    validate_commodity = BashOperator(

        task_id="validate_commodity",

        bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        /opt/spark/jobs/commodity.py
        """

    )


    # Run one after another

    validate_equity \
        >> validate_bond \
        >> validate_commodity