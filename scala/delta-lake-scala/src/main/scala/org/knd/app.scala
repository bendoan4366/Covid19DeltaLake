package org.knd

import scala.util.Properties
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SQLContext}
import io.delta.tables._
import org.apache.spark.sql.functions._
import schemas._

object app {

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

    // read and transform cases data
    val df_cases_data_raw = spark.read
      .option("header", true)
      .schema(schemas.cases_schema)
      .csv("s3a://covid19-lake/rearc-covid-19-nyt-data-in-usa/csv/us-counties/*.csv")

    df_cases_data_raw.show(10)
    df_cases_data_raw.printSchema()

    val df_cases_data_final = transformer.transformCasesDf(df_cases_data_raw, spark)

    // read and transform testing data
    val df_testing_data_raw = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("s3a://covid19-lake/rearc-covid-19-testing-data/csv/states_daily/*.csv")

    val df_test_data_final = transformer.transformTestsDf(df_testing_data_raw)


    // read predictions data
    val df_predictions_data_final = spark.read
      .schema(schemas.prediction_schema)
      .json("s3a://covid19-lake/rearc-covid-19-prediction-models/json/county-predictions/*.json")


    //read populations data
    val df_county_populations_final = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("s3a://covid19-lake/static-datasets/csv/CountyPopulation/*.csv")


    df_cases_data_final.printSchema()
    df_cases_data_final.show(5)

    df_test_data_final.printSchema()
    df_test_data_final.show(5)


  }

}
