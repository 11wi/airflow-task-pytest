# -*- coding: utf-8 -*-


import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from task_function import func1, func2, func3

default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": pendulum.duration(seconds=10),
    "provide_context": True,
}

bucket = "mybuqet"


def create_dag(
    dag_id, description="", tags=[], schedule_interval=None, start_date=pendulum.now()
) -> DAG:
    with DAG(
        default_args=default_args,
        tags=tags,
        dag_id=dag_id,
        description=description,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False,
    ) as dag:
        f1 = PythonOperator(
            python_callable=func1, task_id="f1", op_kwargs={"bucket": bucket}
        )

        f2 = PythonOperator(
            python_callable=func2, task_id="f2", op_kwargs={"bucket": bucket}
        )

        f3 = PythonOperator(
            python_callable=func3, task_id="f3", op_kwargs={"bucket": bucket}
        )

        f1 >> f2 >> f3
        return dag


globals()["dag"] = create_dag(
    dag_id="mydag",
    start_date=pendulum.datetime(2021, 8, 1, tz="Asia/Seoul"),
)
