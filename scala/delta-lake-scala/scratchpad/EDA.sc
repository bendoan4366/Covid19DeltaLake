import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper

import scala.util.Properties
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{to_date, to_timestamp}

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

val prediction_schema = StructType(
  List(
    StructField("countyfips", StringType),
    StructField("countyname", StringType),
    StructField("predicted_date", DateType),
    StructField("predicted_deaths", DoubleType),
    StructField("severity_county_5-day", DoubleType),
    StructField("statename", StringType),
    )
)



/*


val df_cases_data = spark.read.option("header", true).schema(cases_schema).csv("s3a://covid19-lake/rearc-covid-19-nyt-data-in-usa/csv/us-counties/*.csv")

df_cases_data.printSchema()
df_cases_data.show(10)

val df_testing_data = spark.read.option("header", true).option("inferSchema", true).csv("s3a://covid19-lake/rearc-covid-19-testing-data/csv/states_daily/*.csv")

val df_testing_data_final = df_testing_data
  .withColumn("date", to_date(df_testing_data("date").cast(StringType), "yyyyMMdd"))
  .withColumn("fips", df_testing_data("fips").cast(StringType))
  .drop("lastUpdateET")

df_testing_data_final.show(10)
df_testing_data_final.printSchema()


val df_predictions_data = spark.read.schema(prediction_schema).json("s3a://covid19-lake/rearc-covid-19-prediction-models/json/county-predictions/*.json")

df_predictions_data.show(10)
df_predictions_data.printSchema()



 */
 */
 */
 */

//spark.sparkContext.addFile("https://www.ers.usda.gov/webdocs/DataFiles/48747/PovertyEstimates.csv")
//spark.sparkContext.addFile("https://www.ers.usda.gov/webdocs/DataFiles/48747/Education.csv")
spark.sparkContext.addFile("https://projects.fivethirtyeight.com/polls-page/president_polls.csv")

//val poverty_estimate_data = spark.read.option("header", true).option("inferSchema", true).csv(SparkFiles.get("PovertyEstimates.csv"))
//val education_estimate_data = spark.read.option("header", true).option("inferSchema", true).csv(SparkFiles.get("Education.csv"))
val polling_data = spark.read.option("header", true).option("inferSchema", true).csv(SparkFiles.get("president_polls.csv"))



polling_data.printSchema()
polling_data.show(10)

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

val polling_data_final = polling_data
  .withColumn("question_id", polling_data("question_id").cast(StringType))
  .withColumn("poll_id", polling_data("poll_id").cast(StringType))
  .withColumn("pollster_id", polling_data("pollster_id").cast(StringType))
  .withColumn("start_date", to_date(polling_data("start_date"), "MM/dd/yy"))
  .withColumn("end_date", to_date(polling_data("end_date"), "MM/dd/yy"))
  .withColumn("election_date", to_date(polling_data("election_date"), "MM/dd/yy"))
  .withColumn("created_at", to_timestamp(polling_data("created_at"), "MM/dd/yy HH:mm"))
polling_data_final.printSchema()
polling_data_final.show(10)
