import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from datetime import datetime

#add a time stamp to my file
timestamp = datetime.utcnow().strftime('%m%d%y_%H%M%S')

# required for Glue job execution
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# setup Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Define S3 paths
raw_data_path = 's3://rawdata-aws-airflow/raw_data/data.csv'
output_path = f's3://etldata-aws-airflow/customer_tables/run_{timestamp}/'

# load data
df = spark.read.option('header', 'true').csv(raw_data_path)

##################### data cleaning & process #############################
# convert gender: Female -> 1, Male -> 0
df = df.withColumn('gender', F.when(F.col("gender") == "F", 1).otherwise(0).cast("int"))

# compute account count per customer
acct_counts = df.select("ssn", "acct_num").distinct().groupBy("ssn").agg(F.count("acct_num").alias("acct_count"))

# compute transaction count per customer
trans_counts = df.groupBy("ssn").agg(F.count("trans_num").alias("trans_count"))

# compute address
address = df.withColumn("address", F.concat_ws(",", "street", "city", "state", "zip"))

# select relevant customer fields
customer_info = df.select(
       "ssn", "dob", "gender", "job", "profile",
       "street", "lat", "long", "city_pop"
).dropDuplicates(['ssn'])

# join with account and transaction counts
customer_table = customer_info.join(acct_counts, on="ssn", how="left").join(trans_counts, on="ssn", how="left")

# write to S3 as csv
customer_table.write.mode("overwrite").option("header", "true").csv(output_path)

# Finish
job.commit()