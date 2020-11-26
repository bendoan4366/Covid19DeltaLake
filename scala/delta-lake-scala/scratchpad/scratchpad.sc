import java.io.File

import com.typesafe.config.{ConfigFactory, ConfigParseOptions}
import org.apache.spark
import org.apache.spark.sql.SparkSession

val parseOptions = ConfigParseOptions.defaults()
  .setAllowMissing(false)

val config = ConfigFactory.parseFile(new File("C:\\Users\\kndoa\\OneDrive\\Desktop\\Data_Science\\CovidDeltaLake\\scala\\delta-lake-scala\\src\\main\\resources\\application.conf"),
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


/*
val m1 = Map("fname" -> "Al",
  "lname" -> "Alexander",
  "yeet" -> "yuh",
  "slat" -> "slime")

for ((k,v) <- m1) {
  printf("key: %s, value: %s\n", k, v)
}

 */

var exists = false

try {

  val table = spark.read.parquet(delta_lake_bucket + "cases/*.parquet")
  exists = true
  print("here")

} catch {

  case notFound: org.apache.spark.sql.AnalysisException => exists = false
    print("not here \n")

} finally {

  print("\n done \n")

}

print(exists)
print("yuh")