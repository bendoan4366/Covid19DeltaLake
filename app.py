from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
from read_s3_boto import get_matching_s3_objects, get_matching_s3_keys, get_s3_object_to_df, get_spark_dataframes

spark = SparkSession \
  .builder \
  .appName("LoadDelta") \
  .master("local[*]") \
  .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
  .config('spark.jars.packages', r'C:\Users\kndoa\DevTools\spark-3.0.1-bin-hadoop2.7\jars\aws-java-sdk-1.11.859.jar') \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
  .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
  .getOrCreate()

spark.sparkContext.addPyFile(r"C:\Users\kndoa\DevTools\spark-3.0.1-bin-hadoop2.7\jars\delta-core_2.12-0.7.0.jar")
conf = spark.sparkContext._conf.setAll([('spark.delta.logStore.class','org.apache.spark.sql.delta.storage.S3SingleDriverLogStore')])
spark.sparkContext._conf.getAll()

aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY_ID")

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_secret)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.endpoint", "s3.amazonaws.com")

bucket = "covid19-lake"

df_county_populations = get_spark_dataframes(spark, bucket,  "static-datasets/csv/CountyPopulation", ".csv", "csv")
df_county_populations = df_county_populations.withColumnRenamed("Population Estimate 2018", "2018_pop_estimate")
print("\n" + "df_county_populations - done" + "\n")

df_county_cases = get_spark_dataframes(spark, bucket, "rearc-covid-19-nyt-data-in-usa/csv/us-counties", ".csv", "csv")
print("\n" + "df_county_cases - done...." + "\n")

df_state_tests = get_spark_dataframes(spark, bucket, "rearc-covid-19-testing-data/json/states_daily", ".json", "json")
print("\n" + "state_testing - done...." + "\n")

from delta.tables import *
df_county_populations.write.format("delta").mode("overwrite").save("s3n://covid-delta-lake/test/populations")
df_state_tests.write.format("delta").mode("overwrite").save("s3n://covid-delta-lake/test/tests")
df_county_cases.write.format("delta").mode("overwrite").save("s3n://covid-delta-lake/test/cases")