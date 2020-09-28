import boto3
import pandas as pd
from pyspark import SparkContext, SparkConf
import pyspark.sql
from io import StringIO

from pyspark.sql import SparkSession

client = boto3.client("s3")
paginator = client.get_paginator("list_objects_v2")

def get_matching_s3_objects(bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                break

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj


def get_matching_s3_keys(bucket, prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(bucket, prefix, suffix):
        return obj["Key"]


def get_s3_object_to_df(bucket, key, format):
    """
    given a bucket, key and format, extracts body from the s3 object, and converts to pandas dataframe

    param bucket: Name of the S3 bucket.
    param key: key for object in s3, generated from invoking get_matching_s3_keys
    param format: format of the data being read from body of object (currently csv and json are the only two supported)
    """
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    body = obj.get()['Body'].read()

    object_string = bytes.decode(body)

    if format == "json":
        df = pd.read_json(object_string, lines=True)
    elif format == "csv":
        csv_data = StringIO(object_string)
        df = pd.read_csv(csv_data)
    else:
        print("invalid format, specify csv or json. Future data structures to come..")

    return df


def get_spark_dataframes(spark_session, bucket, prefix, suffix, format):
    """
    invokes all functions above to locate an item in s3, extract the key, extract the contents, and write to a spark dataframe

    param spark_session: Name of spark session
    param bucket: Name of the S3 bucket.
    param prefix: Only fetch keys that start with this prefix (optional).
    param suffix: Only fetch keys that end with this suffix (optional).
    param format: format of the data being read from body of object (currently csv and json are the only two supported)
    """
    item_key = get_matching_s3_keys(bucket, prefix, suffix)
    df = get_s3_object_to_df(bucket, item_key, format)

    spark_df = spark_session.createDataFrame(df)

    return spark_df
