package org.tfmupm

import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.functions.{coalesce, col, collect_list, expr, first, from_json, lit, window}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

import java.io.File

object KafkaReaderWriter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Reading from kafka topic")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "nifitopic")
      .load()

    val schema1 = StructType(Seq(
      StructField("subject_id", StringType),
      StructField("record_id", StringType),
      StructField("name", StringType),
      StructField("birth_year", IntegerType),
      StructField("diagnosis", StringType),
      StructField("gender", StringType),
      StructField("dominant_hand", StringType),
      StructField("record_added_on", StringType),
      StructField("recorded_tasks", ArrayType(StructType(Seq(
        StructField("accelerometer_filename", StringType),
        StructField("gyroscope_filename", StringType),
        StructField("accelerometer_values", StringType),
        StructField("gyroscope_values", StringType),
        StructField("task_id", StringType),
        StructField("task_name", StringType),
        StructField("trial", IntegerType),

      ))))
    ))

    val json_df = df.selectExpr("cast(value as string) as value")
    val json_expanded_df = json_df
      .withColumn("value", from_json(col("value"), schema1))
      .select("value.*")

    json_expanded_df
      .writeStream
      .outputMode("append")
      .format("delta")
      .option("checkpointLocation", "D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/bronze-checkpoint")
      .start("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/bronze")

    val query = json_expanded_df
      .writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()

  }
}
