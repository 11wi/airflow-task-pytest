# airflow unit tests using pytest and fake s3

for easy development and testing, test each task with xcom from predefined DAG

for airflow setup, <https://airflow.apache.org/docs/apache-airflow/stable/start/local.html> `export AIRFLOW_HOME=$(pwd); export PYTHONPATH=$(pwd); mkdir dags; ln -s $(pwd)/dag.py $(pwd)/dags` 

test env `pytest` 

production env `airflow dags test mydag 2021-08-01`
