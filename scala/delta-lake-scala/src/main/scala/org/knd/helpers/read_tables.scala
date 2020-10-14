package org.knd.helpers

import org.apache.spark.SparkFiles
import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object read_tables {


  // read and transform cases data
  def read_cases_table(spark: SparkSession): DataFrame = {

    val df_cases_data_raw = spark.read
      .option("header", true)
      .schema(schemas.cases_schema)
      .csv("s3a://covid19-lake/rearc-covid-19-nyt-data-in-usa/csv/us-counties/*.csv")

    val state_abbr_mappings = spark.read
      .option("header", true)
      .csv("../delta-lake-scala/src/main/scala/static_files/state_mappings.csv")
      .select("State", "Code")
      .withColumnRenamed("State", "source_state")

    val df_cases_data_final = df_cases_data_raw.join(state_abbr_mappings, df_cases_data_raw("state") === state_abbr_mappings("source_state"))

    return df_cases_data_final
  }


  // read and transform testing data
  def read_tests_table(spark: SparkSession): DataFrame = {

    val df_testing_data_raw = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("s3a://covid19-lake/rearc-covid-19-testing-data/csv/states_daily/*.csv")

    val df_test_data_final = df_testing_data_raw
      .withColumn("date", to_date(df_testing_data_raw("date").cast(StringType), "yyyyMMdd"))
      .withColumn("fips", df_testing_data_raw("fips").cast(StringType))
      .drop("lastUpdateET")

    return df_test_data_final
  }


  // read predictions data
  def read_predictions_table(spark: SparkSession): DataFrame = {

    val df_predictions_data_final = spark.read
    .schema (schemas.prediction_schema)
    .json ("s3a://covid19-lake/rearc-covid-19-prediction-models/json/county-predictions/*.json")

    return df_predictions_data_final
  }


  //read populations data
  def read_populations_table(spark: SparkSession): DataFrame = {

    val df_county_populations_raw = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("s3a://covid19-lake/static-datasets/csv/CountyPopulation/*.csv")

    val df_county_populations_final = df_county_populations_raw
      .withColumnRenamed("Population Estimate 2018","population_estimate_2018")
    return df_county_populations_final

  }


  //read poverty data
  def read_poverty_table(spark: SparkSession): DataFrame = {

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")

    spark.sparkContext.addFile("https://www.ers.usda.gov/webdocs/DataFiles/48747/PovertyEstimates.csv")
    val df_poverty_estimate_data_raw = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(SparkFiles.get("PovertyEstimates.csv"))

    val df_poverty_estimate_data_final = df_poverty_estimate_data_raw
      .withColumn("fips", df_poverty_estimate_data_raw("FIPStxt").cast(StringType))


    return df_poverty_estimate_data_final
  }


  //read education level data
  def read_education_table(spark: SparkSession): DataFrame = {

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")

    spark.sparkContext.addFile("https://www.ers.usda.gov/webdocs/DataFiles/48747/Education.csv")
    val df_education_estimate_data_raw = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(SparkFiles.get("Education.csv"))

    val df_education_estimate_data_final = df_education_estimate_data_raw
      .withColumn("fips", df_education_estimate_data_raw("fips").cast(StringType))

    return df_education_estimate_data_final
  }


  //read polling data
  def read_polling_table(spark: SparkSession): DataFrame = {

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")
    spark.sparkContext.addFile("https://projects.fivethirtyeight.com/polls-page/president_polls.csv")

    val df_polling_data_raw = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(SparkFiles.get("president_polls.csv"))

    val df_polling_data_final = df_polling_data_raw
      .withColumn("question_id", df_polling_data_raw("question_id").cast(StringType))
      .withColumn("poll_id", df_polling_data_raw("poll_id").cast(StringType))
      .withColumn("pollster_id", df_polling_data_raw("pollster_id").cast(StringType))
      .withColumn("start_date", to_date(df_polling_data_raw("start_date"), "MM/dd/yy"))
      .withColumn("end_date", to_date(df_polling_data_raw("end_date"), "MM/dd/yy"))
      .withColumn("election_date", to_date(df_polling_data_raw("election_date"), "MM/dd/yy"))
      .withColumn("created_at", to_timestamp(df_polling_data_raw("created_at"), "MM/dd/yy HH:mm"))

    return df_polling_data_final

  }

}
