package org.knd.helpers
import io.delta.tables._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object update_tables {

  def update_tables(spark:SparkSession, origin_table_path:String, update_table:DataFrame) = {

    val delta_table = DeltaTable.forPath(spark, origin_table_path)

    // loop through column of table names and map using loop where list[0] -> list[0]

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
  }
}
