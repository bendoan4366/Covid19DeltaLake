import java.io.File

import com.typesafe.config.{ConfigFactory, ConfigParseOptions}
import io.delta.tables.DeltaTable
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.functions.col

/*
val parseOptions = ConfigParseOptions.defaults()
  .setAllowMissing(false)

val config = ConfigFactory.parseFile(new File("C:\\Users\\kndoa\\OneDrive\\Desktop\\Data_Science\\CovidDeltaLake\\scala\\delta-lake-scala\\src\\main\\resources\\application.conf"),
  parseOptions).resolve()

val delta_config = config.getConfig("delta")
val aws_config = delta_config.getConfig("aws")

val aws_access_key = aws_config.getString("aws_access_key")
val aws_secret = aws_config.getString("aws_secret")
val delta_lake_bucket = aws_config.getString("delta_lake_bucket")


//uncomment to use environment variables instead of config file
val aws_access_key = scala.util.Properties.envOrElse("AWS_ACCESS_KEY", "")
val aws_secret = scala.util.Properties.envOrElse("AWS_SECRET_ACCESS_KEY_ID", "")
*/


// <-------------------------------- CREATE AND CONFIGURE SPARK SESSION ------------------------------------->
val spark = SparkSession
  .builder()
  .appName("scratchpad")
  .master("local")
  .getOrCreate()

val aws_access_key = scala.util.Properties.envOrElse("AWS_ACCESS_KEY", "")
val aws_secret = scala.util.Properties.envOrElse("AWS_SECRET_ACCESS_KEY_ID", "")

spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", aws_access_key)
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", aws_secret)
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

/*
val test_table = spark
  .read
  .format("csv")
  .option("header", true)
  .load("s3a://doan-test-bucket/raw-input/*")

print("read complete")

test_table.show(10)

test_table
  .write
  .format("delta")
  .mode("overwrite")
  .save("s3a://doan-test-bucket/delta-input")

 */
 */

val update_table = spark
  .read
  .format("csv")
  .option("header", true)
  .load("s3a://doan-test-bucket/update/*")

update_table.printSchema()
update_table.show(10)

print("\n ================================================================ \n")

val delta_table = DeltaTable.forPath(spark, "s3a://doan-test-bucket/delta-input")

delta_table.as("origin")
  .merge(
    update_table.as("updates"),
    "origin.id = updates.id")
  .whenMatched.update(Map(
    "id" -> col("updates.id"),
    "cola" -> col("updates.cola"),
    "colb" -> col("updates.colb"),
    "colc" -> col("updates.colc")))
  .whenNotMatched.insert(Map(
    "id" -> col("updates.id"),
    "cola" -> col("updates.cola"),
    "colb" -> col("updates.colb"),
    "colc" -> col("updates.colc")))
  .execute()


print("done!")

val delta_read_table = spark
  .read
  .format("delta")
  .option("header", true)
  .load("s3a://doan-test-bucket/delta-input")

delta_read_table.printSchema()
delta_read_table.show()