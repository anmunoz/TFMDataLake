package org.tfmupm

import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.functions.{col, collect_list, expr, first, from_json, window}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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
      StructField("regularexp", StringType),
      StructField("http.multipart.name", StringType)
    ))

    val json_df = df.selectExpr("cast(value as string) as value")
    val json_expanded_df = json_df
      .withColumn("value", from_json(col("value"), schema1))
      .select("value.*")

    json_expanded_df
      .writeStream
      .outputMode("append")
      .format("delta")
      .option("checkpointLocation", "D:/Archivos uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/bronze-checkpoint")
      .start("D:/Archivos uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/bronze")


    val query = json_expanded_df
      .writeStream
      .format("console")
      .outputMode("append")
      .start()


    query.awaitTermination()
  }
}
