import awswrangler as wr
import boto3
import pendulum
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from moto import mock_s3

from dag import create_dag, bucket
from task_function import settings

mock = mock_s3()
mock.start()

boto3.setup_default_session(
    region_name=settings.get("AWS_REGION"),
    aws_access_key_id=settings.get("AWS_ACCESS_KEY"),
    aws_secret_access_key=settings.get("AWS_SECRET_ACCESS"),
)
boto3.client("s3").create_bucket(Bucket=bucket)

dt = pendulum.datetime(2021, 8, 1, tz="Asia/Seoul")
pendulum.set_test_now(dt)


def test_dag_loaded():
    dagbag = DagBag()
    assert dagbag.import_errors == {}
    dag = create_dag("test")
    assert dag is not None


task_instances = {}


def run(dag, task_id):
    ti = TaskInstance(task=dag.get_task(task_id=task_id), execution_date=dt)
    ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True)
    assert ti.state == State.SUCCESS
    task_instances[task_id] = ti
    return ti.xcom_pull()


def test_f1():
    dag = create_dag("dag_test", start_date=dt)
    f = run(dag, "f1")
    assert wr.s3.read_csv(path=f).shape == (3, 2)


def test_f2():
    dag = create_dag("dag_test", start_date=dt)
    f = run(dag, "f2")
    assert wr.s3.read_csv(path=f).shape == (3, 3)


def test_f3():
    dag = create_dag("dag_test", start_date=dt)
    f = run(dag, "f3")

    df3 = wr.s3.read_csv(path=f)
    df2 = wr.s3.read_csv(path=task_instances["f2"].xcom_pull())
    assert (df3 >= df2).all(axis=None)


def test_dag():
    wr.s3.delete_objects(path=f"s3://{bucket}")
    assert wr.s3.list_objects(path=f"s3://{bucket}") == []

    dag = create_dag("dag_test", start_date=dt)

    xcoms = []
    for task_id in ["f1", "f2", "f3"]:
        xcoms.append(run(dag, task_id))

    assert wr.s3.does_object_exist(xcoms[0])
    assert wr.s3.does_object_exist(xcoms[1])
    assert wr.s3.does_object_exist(xcoms[2])
