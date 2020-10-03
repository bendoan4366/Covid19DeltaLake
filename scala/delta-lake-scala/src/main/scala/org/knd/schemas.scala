package org.knd

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object schemas {

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

}
