import java.io.File
import java.util.Properties

import com.typesafe.config.{ConfigFactory, ConfigParseOptions}

import scala.io.Source
import scala.reflect.internal.util.NoSourceFile.path

val parseOptions = ConfigParseOptions.defaults()
  .setAllowMissing(false)

val config = ConfigFactory.parseFile(new File("C:\\Users\\kndoa\\OneDrive\\Desktop\\Data_Science\\CovidDeltaLake\\scala\\delta-lake-scala\\scratchpad\\application.conf"),
  parseOptions).resolve()

print(config)

val delta_config = config.getConfig("delta")
val aws_config = config.getConfig("aws")

val aws_access_key = aws_config.getString("aws_access_key")
val aws_secret = aws_config.getString("aws_secret")
val delta_lake_bucket = aws_config.getString("delta_lake_bucket")
/*
val properties: Properties = new Properties()

if (url != null) {
  val source = Source.fromURL(url)
  properties.load(source.bufferedReader())
}
else {
  print("no file found")
}

val table = properties.getProperty("hbase_table_name")
val zquorum = properties.getProperty("localhost")
val port = properties.getProperty("2181")

 */