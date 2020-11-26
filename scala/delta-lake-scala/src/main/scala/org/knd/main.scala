package org.knd
import java.io.File

import com.typesafe.config.{ConfigFactory, ConfigParseOptions}
import jdk.internal.net.http.frame.DataFrame

import scala.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.knd.helpers.table_reader

import scala.::
import scala.collection.mutable
import scala.io.Source

object main {

  def main(args: Array[String]) : Unit = {

    // <-------------------------------- READ CONFIGURATIONS ------------------------------------->
    val parseOptions = ConfigParseOptions.defaults()
      .setAllowMissing(false)

    val config = ConfigFactory.parseFile(new File("src/main/resources/application.conf"),
      parseOptions).resolve()

    val delta_config = config.getConfig("delta")
    val aws_config = delta_config.getConfig("aws")

    val aws_access_key = aws_config.getString("aws_access_key")
    val aws_secret = aws_config.getString("aws_secret")
    val delta_lake_bucket = aws_config.getString("delta_lake_bucket")

    /*
    //uncomment to use environment variables instead of config file
    val aws_access_key = scala.util.Properties.envOrElse("AWS_ACCESS_KEY", "")
    val aws_secret = scala.util.Properties.envOrElse("AWS_SECRET_ACCESS_KEY_ID", "")
    */


    // <-------------------------------- CREATE AND CONFIGURE SPARK SESSION ------------------------------------->
    val spark = SparkSession
      .builder()
      .appName("covid-delta-lake")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", aws_access_key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", aws_secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")


    // <-------------------------------- READ AND LOAD TABLES ------------------------------------->
    //read tables from various sources and apply transformations as defined in table_reader.scala
    val cases_table = table_reader.read_cases_table(spark)
    val tests_table = table_reader.read_tests_table(spark)
    val predictons_table = table_reader.read_predictions_table(spark)
    val polls_table = table_reader.read_polling_table(spark)
    val education_estimates_table = table_reader.read_tests_table(spark)
    val poverty_estimates_table = table_reader.read_poverty_table(spark)
    val populations_table = table_reader.read_populations_table(spark)
    val demographics_table = table_reader.read_demographic_data(spark)

    //map tables to s3 links
    val delta_table_links = Map(
      cases_table -> (delta_lake_bucket + "cases/"),
      tests_table -> (delta_lake_bucket + "tests/"),
      predictons_table -> (delta_lake_bucket + "predictions/"),
      polls_table -> (delta_lake_bucket + "polls/"),
      education_estimates_table -> (delta_lake_bucket + "education/"),
      poverty_estimates_table -> (delta_lake_bucket + "poverty/"),
      populations_table -> (delta_lake_bucket + "populations/"),
      demographics_table -> (delta_lake_bucket + "population_demographics/")
    )

    // loop through tables,
    // if table exists, update table
    // if table does not exist, create new table
    for((table, url) <- delta_table_links) {
      val exists = check_table_exists(url, spark)

      if (exists == true) {

        print("it's here")
        //update table script here

      } else {

        print("not here")
        table.write.format("delta").mode("overwrite").save(url)

      }

    }
  }

  def check_table_exists(tablePath:String, spark:SparkSession): Boolean = {

    var exists = false

    try {

      val table = spark.read.parquet(tablePath + "*.parquet")
      exists = true

    } catch {

      case notFound: org.apache.spark.sql.AnalysisException => exists = false

    }
    return exists
  }

}
