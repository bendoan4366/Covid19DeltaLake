import scala.util.Properties
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

val spark = SparkSession
  .builder()
  .appName("eda")
  .master("local")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .getOrCreate()

val aws_access_key = scala.util.Properties.envOrElse("AWS_ACCESS_KEY", "notAvailable" )
val aws_secret = scala.util.Properties.envOrElse("AWS_SECRET_ACCESS_KEY_ID", "notAvailable" )

spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", aws_access_key)
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", aws_secret)
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

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

val df_cases_data = spark.read.option("header", true).schema(cases_schema).csv("s3a://covid19-lake/rearc-covid-19-nyt-data-in-usa/csv/us-counties/*.csv")

df_cases_data.printSchema()
df_cases_data.show(10)


val df_testing_data = spark.read.option("header", true).option("inferSchema", true).csv("s3a://covid19-lake/rearc-covid-19-testing-data/csv/states_daily/*.csv")

df_testing_data.show(10)
df_testing_data.printSchema()

