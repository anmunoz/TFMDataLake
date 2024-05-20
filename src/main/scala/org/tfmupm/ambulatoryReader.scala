package org.tfmupm

import org.apache.spark.sql.SparkSession
import io.delta.tables._


object ambulatoryReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Reading from kafka topic")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val dfTableBronzeAmbulatory = spark.read.format("delta").load("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/bronze_ambulatory")
    val dfTableBronzeContinuous = spark.read.format("delta").load("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/bronze_continuous")
    val dfSubjectsTable = spark.read.format("delta").load("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/SubjectsTable")
    val dfSubjectAlberto = spark.read.format("delta").load("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/Subjects/2f73366bc5765cb9bdc1c7f436d03067f200958ff5e7a95635b5577815b62e96")
    val dfSubjectJulian= spark.read.format("delta").load("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/Subjects/bdc1c7f436d03067f20092f73366bc5765cb958ff5e7a95635b5577815b62e96")
    val dfSubjectPepe= spark.read.format("delta").load("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/Subjects/35b5577815b62e962f73366bc5765cb9bdc1c7f436d03067f200958ff5e7a956")
    val dfTasksJulian = spark.read.format("delta").load("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/Subjects/35b5577815b62e962f73366bc5765cb9bdc1c7f436d03067f200958ff5e7a956/tasks")

    dfTableBronzeAmbulatory.show()
    dfTableBronzeContinuous.show()
    dfSubjectsTable.show()
    dfSubjectAlberto.show()
    dfSubjectJulian.show()
    dfSubjectPepe.show()
    dfTasksJulian.show()
  }
}

