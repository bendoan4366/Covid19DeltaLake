package org.knd

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.StringType

object transformer {

  def transformCasesDf(df: DataFrame, spark:SparkSession): DataFrame = {

    val state_abbr_mappings = spark.read
      .option("header", true)
      .csv("../delta-lake-scala/src/main/scala/static_files/state_mappings.csv")
      .select("State", "Code")
      .withColumnRenamed("State", "source_state")
"C:\\Users\\kndoa\\OneDrive\\Desktop\\Data_Science\\CovidDeltaLake\\scala\\delta-lake-scala\\src\\main\\scala\\static_files\\state_mappings.csv"
    val final_cases_df = df.join(state_abbr_mappings, df("state") === state_abbr_mappings("source_state"))

    return final_cases_df
  }

  def transformTestsDf(df: DataFrame): DataFrame = {

    val final_testing_by_state_df = df
      .withColumn("date", to_date(df("date").cast(StringType), "yyyyMMdd"))
      .withColumn("fips", df("fips").cast(StringType))
      .drop("lastUpdateET")

    return final_testing_by_state_df

  }

}
