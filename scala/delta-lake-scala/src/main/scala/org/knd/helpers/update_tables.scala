package org.knd.helpers
import io.delta.tables._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object update_tables {

  def update_tables(spark:SparkSession, origin_table_path:String, update_table:DataFrame) = {

    DeltaTable.forPath (spark, origin_table_path)
    .as ("origin")
    .merge (
    update_table.as ("updates"),
    "events.eventId = updates.eventId")
    .whenMatched
    .updateExpr (
    Map ("data" -> "updates.data") )
    .whenNotMatched
    .insertExpr (
    Map (
    "date" -> "updates.date",
    "eventId" -> "updates.eventId",
    "data" -> "updates.data") )
    .execute ()
  }

}
