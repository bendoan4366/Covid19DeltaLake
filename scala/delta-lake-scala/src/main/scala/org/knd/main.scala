package org.knd
import org.knd.helpers.read_tables._
import org.apache.spark.sql.SparkSession
import org.knd.helpers.read_tables


object main {

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

  //read tables from various sources
  val cases_table = read_tables.read_cases_table(spark)
  val tests_table = read_tables.read_tests_table(spark)
  val predictons_table = read_tables.read_predictions_table(spark)
  val polls_table = read_tables.read_polling_table(spark)
  val education_estimates_table = read_tables.read_tests_table(spark)
  val poverty_estimates_table = read_tables.read_poverty_table(spark)
  val populations_table = read_tables.read_populations_table(spark)

  //write tables to s3 in delta table format
  cases_table.write.format("delta").mode("overwrite").save("s3a://covid-delta-lake/cases/")
  tests_table.write.format("delta").mode("overwrite").save("s3a://covid-delta-lake/tests/")
  predictons_table.write.format("delta").mode("overwrite").save("s3a://covid-delta-lake/predictions/")
  polls_table.write.format("delta").mode("overwrite").save("s3a://covid-delta-lake/polls/")
  education_estimates_table.write.format("delta").mode("overwrite").save("s3a://covid-delta-lake/education/")
  poverty_estimates_table.write.format("delta").mode("overwrite").save("s3a://covid-delta-lake/poverty/")
  populations_table.write.format("delta").mode("overwrite").save("s3a://covid-delta-lake/populations/")

  }

}
