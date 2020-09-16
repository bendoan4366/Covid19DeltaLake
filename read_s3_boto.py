import boto3
import pandas as pd
from pyspark import SparkContext, SparkConf
import pyspark.sql

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
        yield obj["Key"]


def get_s3_object_to_df(bucket, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    body = obj.get()['Body'].read()

    json_string = bytes.decode(body)
    df = pd.read_json(json_string, lines=True)

    return df


def get_spark_dataframes(spark_session, bucket, key):
    item_key = get_matching_s3_keys(bucket, key)
    df = get_s3_object_to_df(bucket, key)

    spark_df = spark_session.createDataFrame(df)

    return spark_df