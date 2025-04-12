from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'jingwei',
    'start_date': datetime(2025,4,8),
    'retries': 3,
    'retry_delay':timedelta(minutes=5)
}

with (DAG(
    dag_id='etl_glue_job',
    default_args=default_args,
    description='Run AWS Glue to transform and create Tables',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'customer', 'transaction', 'merchant']
) as dag):
    upload_script_to_s3 = BashOperator(
        task_id='upload_transform_scripts',
        bash_command='aws s3 cp /opt/airflow/glue_jobs/ s3://glue-script-aws-airflow/scripts/ --recursive'
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

    run_merchant_glue_job = GlueJobOperator(
        task_id='run_merchant_glue_job',
        job_name='transform-merchant-job',
        script_location='s3://glue-script-aws-airflow/scripts/transform_merchant_metrics.py',
        iam_role_name='AWSGlueServiceRole-airflow-jingwei',
        region_name='us-east-1',
        create_job_kwargs={
            'GlueVersion': '5.0',
            'NumberOfWorkers': 2,
            'WorkerType': "G.1X"
        }
    )

    run_account_glue_job = GlueJobOperator(
        task_id='run_account_glue_job',
        job_name='transform-account-job',
        script_location='s3://glue-script-aws-airflow/scripts/transform_account.py',
        iam_role_name='AWSGlueServiceRole-airflow-jingwei',
        region_name='us-east-1',
        create_job_kwargs={
            'GlueVersion': '5.0',
            'NumberOfWorkers': 2,
            'WorkerType': "G.1X"
        }
    )



    upload_script_to_s3.set_downstream(run_customer_glue_job)
    run_customer_glue_job >> run_transaction_glue_job >> [run_account_glue_job, run_merchant_glue_job]