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
output_path = f's3://etldata-aws-airflow/account_tables/run_{timestamp}/'

# load data
df = spark.read.option('header', 'true').csv(raw_data_path)

account_table = (
    df.select("acct_num", "ssn")
      .dropna()
      .dropDuplicates(["acct_num"])
)

account_table.write.mode("overwrite").option("header", "true").csv(output_path)

# finish job
job.commit()