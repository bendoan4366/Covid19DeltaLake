from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
from read_s3_boto import get_matching_s3_objects, get_matching_s3_keys, get_s3_object_to_df, get_spark_dataframes

sc = SparkContext.getOrCreate()

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

spark.sparkContext.addPyFile(r"C:\Users\kndoa\DevTools\spark-3.0.1-bin-hadoop2.7\jars\delta-core_2.12-0.7.0.jar")
from delta.tables import *

aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY_ID")

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_access_key)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_secret)
sc._jsc.hadoopConfiguration().set("fs.s3n.endpoint", "s3.amazonaws.com")

bucket = "covid19-lake"
print("\n" + "df_county_populations - starting...." + "\n")
df_county_populations = get_spark_dataframes(spark, bucket,  "static-datasets/csv/CountyPopulation", ".csv", "csv")
print("\n" + "df_county_populations - done" + "\n")

print("\n" + "df_county_cases - starting...." + "\n")
df_county_cases = get_spark_dataframes(spark, bucket, "rearc-covid-19-nyt-data-in-usa/csv/us-counties", ".csv", "csv")
print("\n" + "df_county_cases - done...." + "\n")

print("\n" + "state_testing - starting...." + "\n")
df_state_tests = get_spark_dataframes(spark, bucket, "rearc-covid-19-testing-data/json/states_daily", ".json", "json")
print("\n" + "state_testing - done...." + "\n")

df_county_populations.printSchema()
df_county_populations.show(10)

df_county_cases.printSchema()
df_county_cases.show(10)

df_state_tests.printSchema()
df_state_tests.show(10)