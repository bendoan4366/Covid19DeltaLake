import org.apache.spark.rdd.RDD

import scala.util.Properties
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{to_date, to_timestamp}

val spark = SparkSession
  .builder()
  .appName("test_parse")
  .master("local")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .getOrCreate()

spark.sparkContext.addFile("https://www.bls.gov/web/metro/laucntycur14.txt")
//val unemployment_data = spark.sparkContext.textFile(SparkFiles.get("laucntycur14.txt"))

val unemployment_text = spark.sparkContext.textFile(SparkFiles.get("laucntycur14.txt"))
val trash_rows = unemployment_text.take(10)

trash_rows(0)
trash_rows(1)
trash_rows(2)
trash_rows(3)
trash_rows(4)
trash_rows(5)
trash_rows(6)

val da = unemployment_text.mapPartitionsWithIndex{ (id_x, iter) => if (id_x == 0) iter.drop(6) else iter }
val rowRdd: RDD[Row] = da.map(x=>Row(x(0), x(1)))

val schema = StructType(
  List(
    StructField("value1", StringType),
    StructField("value2", StringType),
  )
)

val dataFrame_new: DataFrame = spark.createDataFrame(rowRdd, schema)

