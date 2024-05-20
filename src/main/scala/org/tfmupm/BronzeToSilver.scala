package org.tfmupm
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.delta.DeltaTable

import java.io.File
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.nio.file.{Files, Paths}

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


    // Se borran los datos de la carpeta data que no son la carpeta bronze. Sirve para automatizar el proceso de limpieza de datos.
    val dataPath = "D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data"
    if (Files.exists(Paths.get(dataPath))) {
      val directory = new File(dataPath)
      directory.listFiles().filter(_.isDirectory).filterNot(_.getName.startsWith("bronze")).foreach { folder =>
        folder.listFiles().foreach(_.delete())
        folder.delete()
        println("Se han borrado las carpetas necesarias")
      }
    }


    // Se leen las tablas bronze que contiene todos los datos obtenidos en KafkaReaderWriter tanto de registros Ambulatory como Continuous
    val dfTableBronzeAmbulatory = spark.read.format("delta").load("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/bronze_ambulatory")
    val dfTableBronzeContinuous = spark.read.format("delta").load("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/bronze_continuous")
    // Se guarda en la variable subjectTableA los datos de la tabla bronze Ambulatory y en la variable subjectTableB los datos de la tabla bronze Continuous
    // que se quieren guardar en la tabla silver. Se combinan para obtener la tabla de todos los sujetos.
    val subjectTableA = dfTableBronzeAmbulatory.select("subject_id", "name", "diagnosis", "birth_year").dropDuplicates(Seq("subject_id", "name", "diagnosis", "birth_year"))
    val subjectTableC = dfTableBronzeContinuous.select("subject_id", "name", "diagnosis", "birth_year").dropDuplicates(Seq("subject_id", "name", "diagnosis", "birth_year"))
    val combinedSubjectTable = subjectTableA.union(subjectTableC)
    val combinedSubjectTableNoDuplicates = combinedSubjectTable.dropDuplicates(Seq("subject_id", "name", "diagnosis", "birth_year"))
    // Se guarda la tabla silver en la carpeta SubjectsTable
    combinedSubjectTableNoDuplicates.write.format("delta").mode("overwrite").save("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/SubjectsTable")
    val subjectTableRead = spark.read.format("delta").load("D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data/SubjectsTable")
    // Se muestra la tabla silver
    subjectTableRead.show()


    // Función auxiliar encargada de convertir la fila a tabla
    def writeRowToDeltaAmb(row: org.apache.spark.sql.Row, tableDeltaPath: String): Unit = {
      // Crea el DataFrame a partir de la fila

      val schema = row.schema
      val rdd = spark.sparkContext.parallelize(Seq(row))
      val dfSubjects = spark.createDataFrame(rdd, schema)
      dfSubjects.write.format("delta").mode("overwrite").save(tableDeltaPath)

      val recordedTasksArray = row.getAs[Seq[Row]]("recorded_tasks")

      val schemaAmb = StructType(Seq(
        StructField("accelerometer_filename", StringType, nullable = true),
        StructField("gyroscope_filename", StringType, nullable = true),
        StructField("accelerometer_values", StringType, nullable = true),
        StructField("gyroscope_values", StringType, nullable = true),
        StructField("task_id", StringType, nullable = true),
        StructField("task_name", StringType, nullable = true),
        StructField("trial", IntegerType, nullable = true)
      ))
      // Convertir el array en DataFrame
      val rddTasks = spark.sparkContext.parallelize(recordedTasksArray)
      val dfTasks = spark.createDataFrame(rddTasks, schemaAmb)
      dfTasks.printSchema()
      // Guardar el DataFrame en una tabla Delta
      dfTasks.write.format("delta").mode("overwrite").save(s"$tableDeltaPath/tasks")
    }

    // Función auxiliar encargada de convertir la fila a tabla
    def writeRowToDeltaCont(row: org.apache.spark.sql.Row, tableDeltaPath: String): Unit = {
      // Crea el DataFrame a partir de la fila

      val schema = row.schema
      val rdd = spark.sparkContext.parallelize(Seq(row))
      val dfSubjects = spark.createDataFrame(rdd, schema)
      dfSubjects.write.format("delta").mode("overwrite").save(tableDeltaPath)

      val recordedTasksArray = row.getAs[Seq[Row]]("recorded_tasks")

      val schemaCont = StructType(Seq(
        StructField("task_id", StringType, nullable = true),
        StructField("task_name", StringType, nullable = true),
        StructField("starts_at", StringType, nullable = true),
        StructField("ends_at", StringType, nullable = true)
      ))
      // Convertir el array en DataFrame
      val rddTasks = spark.sparkContext.parallelize(recordedTasksArray)
      val dfTasks = spark.createDataFrame(rddTasks, schemaCont)
      dfTasks.printSchema()
      // Guardar el DataFrame en una tabla Delta
      dfTasks.write.format("delta").mode("overwrite").save(s"$tableDeltaPath/tasks")
    }

    // Por cada fila que tenga la tabla bronze ambulatory, crea una tabla de cada uno de los sujetos.
    def createDeltaTablesForRowsAmb(dataPath: String): Unit = {
      // Itera sobre cada fila de la tabla Delta Lake
      dfTableBronzeAmbulatory.collect().foreach { row =>
        // Escribe la fila en una tabla Delta Lake
        val subjectId = row.getAs[String]("subject_id")
        val tableId = subjectId.replaceAll("/", "_")
        val tableDeltaPath = s"$dataPath/Subjects/$tableId"
        writeRowToDeltaAmb(row, tableDeltaPath)
      }
    }

    // Por cada fila que tenga la tabla bronze continuous, crea una tabla de cada uno de los sujetos.
    def createDeltaTablesForRowsCont(dataPath: String): Unit = {
      // Itera sobre cada fila de la tabla Delta Lake
      dfTableBronzeContinuous.collect().foreach { row =>
        // Escribe la fila en una tabla Delta Lake
        val subjectId = row.getAs[String]("subject_id")
        val tableId = subjectId.replaceAll("/", "_")
        val tableDeltaPath = s"$dataPath/Subjects/$tableId"
        writeRowToDeltaCont(row, tableDeltaPath)
      }
    }
    createDeltaTablesForRowsAmb(dataPath)
    createDeltaTablesForRowsCont(dataPath)
    spark.stop()
  }
}