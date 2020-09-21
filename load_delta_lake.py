import os
import boto3
from datetime import datetime
from pyspark.sql import SparkSession
from helpers.read_s3_boto import get_spark_dataframes

#config spark session
spark = SparkSession \
  .builder \
  .appName("LoadDelta") \
  .master("local[*]") \
  .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
  .config('spark.jars.packages', r'[*****YOUR PATH HERE****]\jars\aws-java-sdk-1.11.859.jar') \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
  .getOrCreate()

spark.sparkContext.addPyFile(r"[*****YOUR PATH HERE****]\jars\delta-core_2.12-0.7.0.jar")
conf = spark.sparkContext._conf.setAll([('spark.delta.logStore.class','org.apache.spark.sql.delta.storage.S3SingleDriverLogStore')])
spark.sparkContext._conf.getAll()

#get aws access keys from environment and set hadoop context
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY_ID")

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_secret)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.endpoint", "s3.amazonaws.com")

#set bucket for source data
bucket = "covid19-lake"

#read from source data to dataframe and transform schema to comply with AWS naming standards
df_state_codes = spark.read.option("header", True).csv("s3n://covid-delta-lake/static/*.csv")
df_state_codes = df_state_codes.withColumnRenamed("State", "state_full")

df_county_populations = get_spark_dataframes(spark, bucket,  "static-datasets/csv/CountyPopulation", ".csv", "csv")
df_county_populations = df_county_populations.withColumnRenamed("Population Estimate 2018", "pop_estimate_2018")
print("\n" + "df_county_populations - done" + "\n")

df_county_cases = get_spark_dataframes(spark, bucket, "rearc-covid-19-nyt-data-in-usa/csv/us-counties", ".csv", "csv")
df_county_cases = df_county_cases.join(df_state_codes.select("state_full", "Code"), df_state_codes["state_full"] == df_county_cases["state"], "inner").select("date", "county", "state_full", "fips", "cases", "deaths", "code")
print("\n" + "df_county_cases - done...." + "\n")

df_state_tests = get_spark_dataframes(spark, bucket, "rearc-covid-19-testing-data/json/states_daily", ".json", "json")
print("\n" + "state_testing - done...." + "\n")

#write delta lake files to s3
from delta.tables import *
df_county_populations.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3n://covid-delta-lake/delta/populations")
df_state_tests.write.format("delta").mode("overwrite").save("s3n://covid-delta-lake/delta/tests")
df_county_cases.write.format("delta").mode("overwrite").save("s3n://covid-delta-lake/delta/cases")


#move delta log files to separate s3 partition
client = boto3.client("s3")
bucket = "covid-delta-lake"
s3 = boto3.resource('s3')

prefixes = ['delta/tests/', 'delta/populations/', 'delta/cases/']
for pref in prefixes:
    for bucket in s3.buckets.all():
        for obj in bucket.objects.filter(Prefix=pref):
            key = obj.key
            if "_delta" in key:
                # Copy object A as object B
                print(("covid-delta-lake" + "/" +key))
                s3.Object("covid-delta-lake", "log_files" + "/" + key).copy_from(CopySource="covid-delta-lake/" + str(key))
                s3.Object("covid-delta-lake", str(key)).delete()