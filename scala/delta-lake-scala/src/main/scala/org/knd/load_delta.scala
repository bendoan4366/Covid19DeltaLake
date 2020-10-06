package org.knd

import scala.util.Properties
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.{SQLContext, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.functions._
import schemas._

object load_delta {

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


    //read poverty data
    spark.sparkContext.addFile("https://www.ers.usda.gov/webdocs/DataFiles/*/PovertyEstimates.csv")
    val df_poverty_estimate_data_final = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(SparkFiles.get("PovertyEstimates.csv"))


    //read education level data
    spark.sparkContext.addFile("https://www.ers.usda.gov/webdocs/DataFiles/*/Education.csv")
    val df_education_estimate_data_final = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(SparkFiles.get("Education.csv"))


    //read polling data
    spark.sparkContext.addFile("https://projects.fivethirtyeight.com/polls-page/president_polls.csv")
    val polling_data = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(SparkFiles.get("president_polls.csv"))


  }

}
