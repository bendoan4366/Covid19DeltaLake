import java.io.File

import com.typesafe.config.{ConfigFactory, ConfigParseOptions}
import jdk.internal.net.http.frame.DataFrame

import scala.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.::
import scala.collection.mutable
import scala.io.Source

val spark = SparkSession
  .builder()
  .appName("test")
  .master("local[*]")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .getOrCreate()


val test_table = spark
  .read
  .option("header", true)
  .csv("C:\\Users\\kndoa\\OneDrive\\Desktop\\test\\data_raw\\*")

test_table
  .write
  .format("delta")
  .option("mergeSchema", true)
  .mode("overwrite")
  .save("C:\\Users\\kndoa\\OneDrive\\Desktop\\test\\delta_table")

print("done!")

/*
val delta_table_test = spark
  .read
  .format("delta")
  .load("C:/Users/kndoa/OneDrive/Desktop/test/delta_table")

delta_table_test.show(10)

 */



