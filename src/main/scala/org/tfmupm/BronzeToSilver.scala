package org.tfmupm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.first

object BronzeToSilver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Reading from kafka topic")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val dfTableBronze = spark.read.format("delta").load("D:/Archivos uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/bronze")
    dfTableBronze
      .show()

/*
    // Pivot the DataFrame
    val pivotedDF = dfTableBronze
      .groupBy()
      .pivot("`http.multipart.name`")
      .agg(first("regularexp"))

    // Show the pivoted DataFrame
    // Reorder the columns
    val columns = "id" +: pivotedDF.columns.filter(_ != "id")
    val reorderedDF = pivotedDF.select(columns.head, columns.tail: _*)

    // Show the reordered DataFrame
    reorderedDF.show()

    // Write the DataFrame to the Delta Lake
    reorderedDF
      .write
      .format("delta")
      .mode("overwrite")
      .save("D:/Archivos uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/silver")


 */
    val dfTableSilver = spark.read.format("delta").load("D:/Archivos uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/silver")
    dfTableSilver
      .show()
    // Stop the SparkSession
    spark.stop()
  }
}
