package org.knd
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

class schemas {

  val cases_schema = StructType(
    List(
      StructField("date", DateType),
      StructField("county", StringType),
      StructField("state", StringType),
      StructField("fips", StringType),
      StructField("cases", IntegerType),
      StructField("deaths", IntegerType)
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

  val test_schema = StructType(
    List(
      StructField("date", DateType),
      StructField("state", StringType),
      StructField("positive", DateType),
      StructField("negative", DoubleType),
      StructField("pending", DoubleType),
      StructField("totalTestResults", StringType),
      StructField("hospitalizedCurrently", StringType),
      StructField("hospitalizedCumulative", StringType),
      StructField("inIcuCurrently", StringType),
      StructField("inIcuCumulative", StringType),
      StructField("onVentilatorCurrently", StringType),
      StructField("onVentilatorCumulative", StringType),
      StructField("recovered", StringType),
      StructField("dataQualityGrade", StringType),
      StructField("lastUpdateEt", StringType),
      StructField("dateModified", StringType),
      StructField("checkTimeEt", StringType),
      StructField("death", StringType),
      StructField("hospitalized", StringType),
      StructField("dateChecked", StringType),
      StructField("totalTestsViral", StringType),
      StructField("positiveTestsViral", StringType),
      StructField("negativeTestsViral", StringType),
      StructField("positiveCasesViral", StringType),
      StructField("deathConfirmed", StringType),
      StructField("deathProbable", StringType),
      StructField("totalTestEncountersViral", StringType),
      StructField("totalTestsPeopleViral", StringType),
      StructField("totalTestsAntibody", StringType),
      StructField("positiveTestsPeopleAntibody", StringType),
      StructField("negativeTestsPeopleAntibody", StringType),
      StructField("totalTestsPeopleAntigen", StringType),
      StructField("positiveTestsPeopleAntigen", StringType),
      StructField("totalTestsAntigen", StringType),
      StructField("positiveTestsAntigen", StringType),
      StructField("fips", StringType),
      StructField("positiveIncrease", StringType),
      StructField("negativeIncrease", StringType),
      StructField("total", StringType),
      StructField("totalTestResultsSource", StringType),
      StructField("totalTestResultsIncrease", StringType),
      StructField("posNeg ", StringType),
      StructField("deathIncrease", StringType),
      StructField("hospitalizedIncrease", StringType),
      StructField("hash", StringType),
      StructField("commercialScore", StringType),
      StructField("negativeRegularScore", StringType),
      StructField("positiveScore", StringType),
      StructField("score", StringType),
      StructField("grade", StringType)
    )
  )


  /*
  |-- date: integer (nullable = true)
 |-- state: string (nullable = true)
 |-- positive: integer (nullable = true)
 |-- negative: integer (nullable = true)
 |-- pending: integer (nullable = true)
 |-- totalTestResults: integer (nullable = true)
 |-- hospitalizedCurrently: integer (nullable = true)
 |-- hospitalizedCumulative: integer (nullable = true)
 |-- inIcuCurrently: integer (nullable = true)
 |-- inIcuCumulative: integer (nullable = true)
 |-- onVentilatorCurrently: integer (nullable = true)
 |-- onVentilatorCumulative: integer (nullable = true)
 |-- recovered: integer (nullable = true)
 |-- dataQualityGrade: string (nullable = true)
 |-- lastUpdateEt: string (nullable = true)
 |-- dateModified: timestamp (nullable = true)
 |-- checkTimeEt: string (nullable = true)
 |-- death: integer (nullable = true)
 |-- hospitalized: integer (nullable = true)
 |-- dateChecked: timestamp (nullable = true)
 |-- totalTestsViral: integer (nullable = true)
 |-- positiveTestsViral: integer (nullable = true)
 |-- negativeTestsViral: integer (nullable = true)
 |-- positiveCasesViral: integer (nullable = true)
 |-- deathConfirmed: integer (nullable = true)
 |-- deathProbable: integer (nullable = true)
 |-- totalTestEncountersViral: integer (nullable = true)
 |-- totalTestsPeopleViral: integer (nullable = true)
 |-- totalTestsAntibody: integer (nullable = true)
 |-- positiveTestsAntibody: integer (nullable = true)
 |-- negativeTestsAntibody: integer (nullable = true)
 |-- totalTestsPeopleAntibody: integer (nullable = true)
 |-- positiveTestsPeopleAntibody: integer (nullable = true)
 |-- negativeTestsPeopleAntibody: integer (nullable = true)
 |-- totalTestsPeopleAntigen: integer (nullable = true)
 |-- positiveTestsPeopleAntigen: integer (nullable = true)
 |-- totalTestsAntigen: integer (nullable = true)
 |-- positiveTestsAntigen: integer (nullable = true)
 |-- fips: integer (nullable = true)
 |-- positiveIncrease: integer (nullable = true)
 |-- negativeIncrease: integer (nullable = true)
 |-- total: integer (nullable = true)
 |-- totalTestResultsSource: string (nullable = true)
 |-- totalTestResultsIncrease: integer (nullable = true)
 |-- posNeg: integer (nullable = true)
 |-- deathIncrease: integer (nullable = true)
 |-- hospitalizedIncrease: integer (nullable = true)
 |-- hash: string (nullable = true)
 |-- commercialScore: integer (nullable = true)
 |-- negativeRegularScore: integer (nullable = true)
 |-- negativeScore: integer (nullable = true)
 |-- positiveScore: integer (nullable = true)
 |-- score: integer (nullable = true)
 |-- grade: string (nullable = true)
   */

}
