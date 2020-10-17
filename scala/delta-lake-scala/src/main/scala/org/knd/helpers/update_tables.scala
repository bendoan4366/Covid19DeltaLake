package org.knd.helpers
import io.delta.tables._
import org.apache.spark.sql.functions._

object update_tables {


  val updatesDF = ...  // define the updates DataFrame[date, eventId, data]

  DeltaTable.forPath(spark, "/data/events/")
    .as("events")
    .merge(
      updatesDF.as("updates"),
      "events.eventId = updates.eventId")
    .whenMatched
    .updateExpr(
      Map("data" -> "updates.data"))
    .whenNotMatched
    .insertExpr(
      Map(
        "date" -> "updates.date",
        "eventId" -> "updates.eventId",
        "data" -> "updates.data"))
    .execute()

}
