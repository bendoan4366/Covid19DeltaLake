from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
from read_s3_boto import get_matching_s3_objects, get_matching_s3_keys, get_s3_object

sc = SparkContext.getOrCreate()

spark = SparkSession \
  .builder \
  .appName("ConvertSpark") \
  .master("local[*]") \
  .getOrCreate()

