package org.knd.helpers

import org.apache.spark.sql.types._

object schemas {

  val datasource_URLS = Map(
    "cases_table" -> "s3a://covid19-lake/rearc-covid-19-nyt-data-in-usa/csv/us-counties/*.csv",
    "tests_table" -> "s3a://covid19-lake/rearc-covid-19-testing-data/csv/states_daily/*.csv",
    "predictions_table" -> "s3a://covid19-lake/rearc-covid-19-prediction-models/json/county-predictions/*.json",
    "populations_table" -> "s3a://covid19-lake/static-datasets/csv/CountyPopulation/*.csv",
    "poverty_table" -> "https://www.ers.usda.gov/webdocs/DataFiles/48747/PovertyEstimates.csv",
    "education_table" -> "https://www.ers.usda.gov/webdocs/DataFiles/48747/Education.csv",
    "polling_table" -> "https://projects.fivethirtyeight.com/polls-page/president_polls.csv",
    "demographic_table" -> "https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/asrh/cc-est2019-alldata.csv"
  )

  val cases_schema = StructType(
    List(
      StructField("date", DateType),
      StructField("county", StringType),
      StructField("state", StringType),
      StructField("fips", StringType),
      StructField("cases", IntegerType),
      StructField("deaths", IntegerType),
    )
  )

  val prediction_schema = StructType(
    List(
      StructField("countyfips", DateType),
      StructField("countyname", StringType),
      StructField("predicted_date", DateType),
      StructField("predicted_deaths", DoubleType),
      StructField("severity_county_5-day", DoubleType),
      StructField("statename", StringType)
    )
  )


  val poverty_schema = StructType(
    List(
      StructField("countyfips", DateType),
      StructField("countyname", StringType),
      StructField("predicted_date", DateType),
      StructField("predicted_deaths", DoubleType),
      StructField("severity_county_5-day", DoubleType),
      StructField("statename", StringType)
    )
  )

}
