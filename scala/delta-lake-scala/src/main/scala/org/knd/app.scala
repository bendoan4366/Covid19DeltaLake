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

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", aws_access_key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", aws_secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    print("\n" + "====================HERE====================" + "\n")

    // https://covid19-lake.s3.us-east-2.amazonaws.com/rearc-covid-19-nyt-data-in-usa/json/us-counties/part-00000-333f1f52-c1f9-4870-a29f-d2f09fb8d11c-c000.json
    val df_cases_data = spark.read.json("s3a://covid19-lake/rearc-covid-19-nyt-data-in-usa/json/us-counties/*.json")
    val df_testing_data = spark.read.option("header", true).option("inferSchema", true).csv("s3a://covid19-lake/rearc-covid-19-testing-data/csv/states_daily/*.csv")
    val df_predictions_data = spark.read.json("s3a://covid19-lake/rearc-covid-19-prediction-models/json/county-predictions/*.json")
    val df_county_populations = spark.read.option("header", true).option("inferSchema", true).csv("s3a://covid19-lake/static-datasets/csv/CountyPopulation/*.csv")

    df_cases_data.printSchema()
    df_testing_data.printSchema()
    df_predictions_data.printSchema()
    df_county_populations.printSchema()



  }

}
