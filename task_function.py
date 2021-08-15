import os
import tempfile

import awswrangler as wr
import boto3
from dotenv import dotenv_values

env_file = os.getenv("ENV", ".env.production")
settings = dotenv_values(env_file)

boto3.setup_default_session(
    region_name=settings.get("AWS_REGION"),
    aws_access_key_id=settings.get("AWS_ACCESS_KEY"),
    aws_secret_access_key=settings.get("AWS_SECRET_ACCESS"),
)


def func1(**kwargs):
    _, f = tempfile.mkstemp()
    with open(f, "w") as io:
        io.writelines(["a,b\n", "1,2\n", "3,4\n", "5,6\n"])
    s3_path = f"s3://{kwargs['bucket']}/raw_data.csv"
    wr.s3.upload(local_file=f, path=s3_path)
    return s3_path


def func2(**kwargs):
    ti = kwargs["ti"]
    s3_path = ti.xcom_pull(task_ids="f1")

    df = wr.s3.read_csv(s3_path)
    df["c"] = df["a"] + df["b"]

    s3_path = f"s3://{kwargs['bucket']}/interim_data.csv"
    wr.s3.to_csv(df, path=s3_path, index=False)
    return s3_path


def func3(**kwargs):
    ti = kwargs["ti"]
    s3_path = ti.xcom_pull(task_ids="f2")

    df = wr.s3.read_csv(s3_path)
    df.applymap(lambda x: x ** 2)

    s3_path = f"s3://{kwargs['bucket']}/processed_data.csv"
    wr.s3.to_csv(df, path=s3_path)
