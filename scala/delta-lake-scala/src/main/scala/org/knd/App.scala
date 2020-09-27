package org.knd

import scala.util.Properties
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SQLContext}
import io.delta.tables._
import org.apache.spark.sql.functions._

object App {

  def main(args: Array[String]) : Unit = {

    val spark = SparkSession
      .builder()
      .appName("covid-delta-lake")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val aws_access_key = scala.util.Properties.envOrElse("AWS_ACCESS_KEY", "notAvailable" )
    val aws_secret = scala.util.Properties.envOrElse("AWS_SECRET_ACCESS_KEY_ID", "notAvailable" )

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", aws_access_key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", aws_secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    print("\n" + "====================HERE====================" + "\n")

    val data = spark.read.parquet("s3a://knd-udacity-data-lake-project/artists/artists.parquet/*.parquet")
    data.show(10)


  }

}
