import pyspark.sql
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
#Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

conf = SparkConf().setAppName("DeltaNoms").set("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
sc = SparkContext(conf=conf)

spark = SparkSession \
  .builder \
  .appName("LoadDelta") \
  .master("local[*]") \
  .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
  .config('spark.jars.packages', r'C:\Users\kndoa\DevTools\spark-3.0.1-bin-hadoop2.7\jars\aws-java-sdk-1.11.859.jar') \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
  .config('spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore') \
  .getOrCreate()

aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY_ID")

print(aws_access_key)
print("ooooo yuh dat way")

spark.sparkContext.addPyFile(r"C:\Users\kndoa\DevTools\spark-3.0.1-bin-hadoop2.7\jars\delta-core_2.12-0.7.0.jar")
from delta.tables import *

data = spark.range(0, 5)
#data.write.format("delta").mode(saveMode="overwrite").save(r"C:\Users\kndoa\OneDrive\Desktop\Data_Science\CovidDeltaLake\test")

sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", aws_access_key)
sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", aws_secret)
sc._jsc.hadoopConfiguration().set("fs.s3.endpoint", "s3.amazonaws.com")

print("\n"+"\n"+"======================read from s3======================"+"\n"+"\n")
testing_df = spark.read.json(r"s3://covid19-lake/rearc-covid-19-testing-data/json/states_daily/*.json")
testing_df.printSchema()
testing_df.show(5)
#data.write.format("delta").save(r"s3n://covid-delta-lake/test/csv_files")
print("\n"+"\n"+"======================DONE======================"+"\n"+"\n")

