from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default arguments
default_args = {
    "owner": "rrkts",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 21),
    "retries": 1,
}

# Define DAG
with DAG(
    "dbt_trino_pipeline",
    default_args=default_args,
    schedule_interval=None,  # Manually triggered or set a schedule like "@daily"
    catchup=False,
) as dag:
    
    stg_cus = BashOperator(
        task_id="customer_process",
        bash_command="docker exec dbt_trino bash -c 'cd /usr/app/dbt/raffle_shop && dbt run --select stg_customers'",
    )

    stg_orders = BashOperator(
        task_id="orders_process",
        bash_command="docker exec dbt_trino bash -c 'cd /usr/app/dbt/raffle_shop && dbt run --select stg_orders'",
    )
    
    stg_payments = BashOperator(
        task_id="payments_process",
        bash_command="docker exec dbt_trino bash -c 'cd /usr/app/dbt/raffle_shop && dbt run --select stg_payments'",
    )


    process_customer = BashOperator(
        task_id="write_customer_data",
        bash_command="docker exec dbt_trino bash -c 'cd /usr/app/dbt/raffle_shop && dbt run --select data_processed --target postgres2'",
    )

    stg_cus >> stg_orders >> stg_payments >> process_customer
