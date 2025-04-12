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
output_path = f's3://etldata-aws-airflow/merchant_metrics_tables/run_{timestamp}/'

# load data
df = spark.read.option('header', 'true').csv(raw_data_path)

merchant_metrics_table = (
    df.select("acct_num", "merchant", "merchant_num_trans_1_day", "merchant_num_trans_7_day",
            "merchant_num_trans_30_day", "merchant_risk_1_day", "merchant_risk_7_day",
            "merchant_risk_30_day", "merchant_risk_90_day"
              )
      .dropna()
      .dropDuplicates(["acct_num"])
)

merchant_metrics_table.write.mode("overwrite").option("header", "true").csv(output_path)

# finish job
job.commit()