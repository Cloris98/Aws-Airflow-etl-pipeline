from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'jingwei',
    'start_date': datetime(2025,4,8),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='transaction_etl_glue_job',
    default_args=default_args,
    description='Run AWS Glue to transform and create Transaction Table',
    schedule_interval=None,
    catchup=False,
    tags=['transaction']
) as dag:
    upload_script_to_s3 = BashOperator(
        task_id='upload_transform_transaction_script',
        bash_command='aws s3 cp /opt/airflow/glue_jobs/transform_transaction.py s3://glue-script-aws-airflow/scripts/transform_transaction.py'
    )

    run_transaction_glue_job = GlueJobOperator(
        task_id='run_transaction_glue_job',
        job_name='transform-transaction-job',
        script_location='s3://glue-script-aws-airflow/scripts/transform_transaction.py',
        iam_role_name='AWSGlueServiceRole-airflow-jingwei',
        region_name='us-east-1',
        create_job_kwargs={
            'GlueVersion': '5.0',
            'NumberOfWorkers': 1,
            'WorkerType': "G.1X"
        }
    )

    upload_script_to_s3 >> run_transaction_glue_job