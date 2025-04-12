from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'jingwei',
    'start_date': datetime(2025,4,7),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='customer_etl_glue_job',
    default_args=default_args,
    description='Run AWS Glue to transform and create Customer Table',
    schedule_interval=None,
    catchup=False,
    tags=['customer']
) as dag:
    upload_script_to_s3 = BashOperator(
        task_id='upload_transform_customer_script',
        bash_command='aws s3 cp /opt/airflow/glue_jobs/transform_customer.py s3://glue-script-aws-airflow/scripts/transform_customer.py'
    )

    run_customer_glue_job = GlueJobOperator(
        task_id='run_customer_glue_job',
        job_name='transform-customer-job',
        script_location='s3://glue-script-aws-airflow/scripts/transform_customer.py',
        iam_role_name='AWSGlueServiceRole-airflow-jingwei',
        region_name='us-east-1',
        create_job_kwargs={
            'GlueVersion': '5.0',
            'NumberOfWorkers': 1,
            'WorkerType': "G.1X"
        }
    )

    upload_script_to_s3 >> run_customer_glue_job
