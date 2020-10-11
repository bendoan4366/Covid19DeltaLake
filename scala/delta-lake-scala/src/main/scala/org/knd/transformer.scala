package org.knd

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql.types.StringType

object transformer {

  def transformCasesDf(df: DataFrame, spark:SparkSession): DataFrame = {

    val state_abbr_mappings = spark.read
      .option("header", true)
      .csv("../delta-lake-scala/src/main/scala/static_files/state_mappings.csv")
      .select("State", "Code")
      .withColumnRenamed("State", "source_state")

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

  def transformPovertyDf(df: DataFrame): DataFrame = {

    val final_povery_stats_by_state_df = df
      .withColumn("fips", df("fips").cast(StringType))

    return final_povery_stats_by_state_df

  }

  def transformEducationDf(df: DataFrame): DataFrame = {

    val final_education_stats_df = df
      .withColumn("fips", df("fips").cast(StringType))

    return final_education_stats_df

  }

  def transformPollDf(df: DataFrame): DataFrame = {

    val polling_data_final = df
      .withColumn("question_id", df("question_id").cast(StringType))
      .withColumn("poll_id", df("poll_id").cast(StringType))
      .withColumn("pollster_id", df("pollster_id").cast(StringType))
      .withColumn("start_date", to_date(df("start_date"), "MM/dd/yy"))
      .withColumn("end_date", to_date(df("end_date"), "MM/dd/yy"))
      .withColumn("election_date", to_date(df("election_date"), "MM/dd/yy"))
      .withColumn("created_at", to_timestamp(df("created_at"), "MM/dd/yy HH:mm"))

    return polling_data_final

  }



}
