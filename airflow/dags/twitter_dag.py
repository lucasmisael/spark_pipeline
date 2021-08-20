from os.path import join 
from pathlib import Path
from datetime import datetime
#from spark.transformation import twitter_transform
import repackage
repackage.up()
from airflow.models import DAG
from plugins.operators.twitter_operator import TwitterOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator 
from airflow.utils.dates import days_ago

ARGS = {
    "owner": "airflow",
    "depends_on_past" : False,
    "start_date": days_ago(6)
}
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
PARTITION_FOLDER = "extract_date={{ ds }}"
BASE_FOLDER = join(
    str(Path("~/Documents").expanduser()),
    "/Pessoal/PDI/dataPipeline/datalake{stage}/twitter_aluraonline/{partition}"

)


with DAG(   
    dag_id = "twitter_dag" , 
    default_args=ARGS,
    schedule_interval="0 9 * * *",
    max_active_runs=1 )as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline" ,
        query="AluraOnline",
        file_path=join(
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "AluraOnline_{{ ds_nodash }}.json"
        ),
        start_time=(
                "{{"
                f"execution_date.strftime('{TIMESTAMP_FORMAT}')"
                "}}"
        ),
        end_time=(
                "{{"
                f"next_execution_date.strftime('{TIMESTAMP_FORMAT}')"
                "}}"
        )
    )


    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_aluraonline",
        application= join(
            str((Path(__file__).parents[2])),
            "spark/transformation.py"
        ),
        name="twitter_transformation",
        application_args=[
            "--src",
            BASE_FOLDER.format(stage="bronze" , partition=PARTITION_FOLDER),
            "--dest",
            BASE_FOLDER.format(stage="silver" , partition=""),
            "process-date",
            "{{ ds }}"

        ]
    )

    twitter_operator >> twitter_transform