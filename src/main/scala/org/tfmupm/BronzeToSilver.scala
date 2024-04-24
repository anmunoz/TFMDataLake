package org.tfmupm
import org.apache.spark.sql.delta.DeltaTable

import java.io.File
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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
    // Se lee la tabla bronze que contiene todos los datos obtenidos en KafkaReaderWriter
    val dfTableBronze = spark.read.format("delta").load("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/bronze")
    // Se guarda en la variable subjectTable los datos de la tabla bronze que se quieren guardar en la tabla silver
    val subjectTable = dfTableBronze.select("subject_id", "name", "diagnosis", "birth_year").dropDuplicates(Seq("subject_id", "name", "diagnosis", "birth_year"))
    // Se guarda la tabla silver en la carpeta SubjectsTable
    subjectTable.write.format("delta").mode("overwrite").save("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/SubjectsTable")
    val subjectTableRead = spark.read.format("delta").load("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/SubjectsTable")
    //subjectTableRead.show()
    val basePath = "D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data"
    
    // Función auxiliar encargada de convertir la fila a tabla
    def writeRowToDelta(row: org.apache.spark.sql.Row, tableDeltaPath: String): Unit = {
      // Crea el DataFrame a partir de la fila
      val schema = row.schema
      val rdd = spark.sparkContext.parallelize(Seq(row))
      val df = spark.createDataFrame(rdd, schema)

      // Escribe el DataFrame en formato Delta Lake
      df.write.format("delta").mode("overwrite").save(tableDeltaPath)
      df.show()
    }

    // Por cada fila que tenga la tabla bronze, crea una tabla de cada uno de los sujetos. Se guarda toda la información recibida por Nifi.
    def createDeltaTablesForRows(basePath: String): Unit = {
      // Itera sobre cada fila de la tabla Delta Lake
      dfTableBronze.collect().foreach { row =>
        // Escribe la fila en una tabla Delta Lake
        val subjectId = row.getAs[String]("subject_id")
        val tableId = subjectId.replaceAll("/", "_") // Reemplazar "/" por "_" para que sea un nombre de tabla válido
        val tableDeltaPath = s"$basePath/$tableId"
        writeRowToDelta(row, tableDeltaPath)
      }
    }
    createDeltaTablesForRows(basePath)
    spark.stop()

  }
}
