package org.knd
import java.io.File

import com.typesafe.config.{ConfigFactory, ConfigParseOptions}

import scala.util.Properties
import org.knd.helpers.read_tables._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.knd.helpers.read_tables

import scala.::
import scala.collection.mutable
import scala.io.Source

object main {

  def main(args: Array[String]) : Unit = {


    val parseOptions = ConfigParseOptions.defaults()
      .setAllowMissing(false)

    val config = ConfigFactory.parseFile(new File("src/main/resources/application.conf"),
      parseOptions).resolve()

    print(config)

    val delta_config = config.getConfig("delta")
    val aws_config = delta_config.getConfig("aws")

    val aws_access_key = aws_config.getString("aws_access_key")
    val aws_secret = aws_config.getString("aws_secret")
    val delta_lake_bucket = aws_config.getString("delta_lake_bucket")

    print("\n" + aws_access_key + "\n")
    print("\n" + aws_secret + "\n")
    print("\n" + delta_lake_bucket + "\n")

    /*
    val local_aws_access_key = scala.util.Properties.envOrElse("AWS_ACCESS_KEY", "")
    val local_aws_secret = scala.util.Properties.envOrElse("AWS_SECRET_ACCESS_KEY_ID", "")


    val spark = SparkSession
    .builder()
    .appName("covid-delta-lake")
    .master("local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", local_aws_access_key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", local_aws_secret)
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

     */

    }

}
