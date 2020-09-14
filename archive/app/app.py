from pyspark.sql import SparkSession
import os

spark = SparkSession \
  .builder \
  .appName("LoadDelta") \
  .master("Local[*]") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config('spark.jars.packages', r'C:\Users\kndoa\DevTools\spark-3.0.1-bin-hadoop2.7\jars\aws-java-sdk-1.11.859.jar') \
  .getOrCreate()

aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY_ID")

spark.sparkContext.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", aws_access_key)
spark.sparkContext.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", aws_secret)
spark.sparkContext.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

data = spark.range(0, 5)
data.write.format("csv").save("s3a://covid-delta-lake/test/test.csv")

