package org.knd
import com.typesafe.config.ConfigFactory
import scala.util.Properties

import org.knd.helpers.read_tables._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.knd.helpers.read_tables

import scala.::
import scala.collection.mutable
import scala.io.Source
import scala.reflect.io.File

object main {

  def main(args: Array[String]) : Unit = {

    val spark = SparkSession
    .builder()
    .appName("covid-delta-lake")
    .master("local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

    val aws_access_key = ConfigFactory.load("delta_properties.conf").getString("delta.aws.aws_access_key")
    val aws_secret = ConfigFactory.load("delta_properties.conf").getString("delta.aws.aws_secret")
    val delta_lake_bucket = ConfigFactory.load("delta_properties.conf").getString("delta.aws.url")

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", aws_access_key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", aws_secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    //read tables from various sources
    val cases_table = read_tables.read_cases_table(spark)
    val tests_table = read_tables.read_tests_table(spark)
    val predictons_table = read_tables.read_predictions_table(spark)
    val polls_table = read_tables.read_polling_table(spark)
    val education_estimates_table = read_tables.read_tests_table(spark)
    val poverty_estimates_table = read_tables.read_poverty_table(spark)
    val populations_table = read_tables.read_populations_table(spark)

    val dfs = List(cases_table, tests_table, predictons_table, polls_table, education_estimates_table, populations_table)


    //write tables to s3 in delta table format
    cases_table.write.format("delta").mode("overwrite").save(delta_lake_bucket + "cases/")
    tests_table.write.format("delta").mode("overwrite").save(delta_lake_bucket + "tests/")
    predictons_table.write.format("delta").mode("overwrite").save(delta_lake_bucket + "predictions/")
    polls_table.write.format("delta").mode("overwrite").save(delta_lake_bucket + "polls/")
    education_estimates_table.write.format("delta").mode("overwrite").save(delta_lake_bucket + "education/")
    poverty_estimates_table.write.format("delta").mode("overwrite").save(delta_lake_bucket + "poverty/")
    populations_table.write.format("delta").mode("overwrite").save(delta_lake_bucket + "populations/")

    }

}
