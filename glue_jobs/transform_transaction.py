import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from datetime import datetime


#add a time stamp to my file
timestamp = datetime.utcnow().strftime('%m%d%y_%H%M%S')

# setup Spark and Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define S3 paths
raw_data_path = 's3://rawdata-aws-airflow/raw_data/data.csv'
output_path = f's3://etldata-aws-airflow/transaction_tables/run_{timestamp}/'

# load data
df = spark.read.option('header', 'true').csv(raw_data_path)

##################### data cleaning & process #############################
# compute acct_trans_count
acct_trans_count = df.groupBy("acct_num").agg(F.countDistinct("trans_num").alias("acct_trans_count"))
df_with_counts = df.join(acct_trans_count,on='acct_num', how='left')

# select the final columns for the transaction table
transaction_table = df_with_counts.select(
    "acct_num", "trans_num", "category", "amt", "unix_time",
    "trans_time_is_night", "trans_date_is_weekend",
    "acct_trans_count", "is_fraud"
)

transaction_table.write.mode("overwrite").option("header", "true").csv(output_path)

#finish job
job.commit()
