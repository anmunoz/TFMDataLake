package org.tfmupm

import org.apache.spark.sql.SparkSession
import io.delta.tables._
import java.io.File

object SparkRead {
  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Reading from csv tables")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Directorio raíz que se va a analizar para convertir los datos de los sensores en tablas Delta
    val parentDirectory = new File("D:/Archivos_uni/TFM/dataset/data/ADL_sequences")

    // Comprobación de la existencia del directorio raíz
    if (parentDirectory.exists() && parentDirectory.isDirectory) {
      // Se listan los subdirectorios del directorio raíz, es decir, los pacientes
      val pacientes = parentDirectory.listFiles()

      // Se itera sobre cada paciente/subdirectorios
      pacientes.foreach { paciente =>
        // Se vuelve a comprobar si es un directorio
        if (paciente.isDirectory) {
          val pacienteName = paciente.getName
          println(s"Procesando paciente: $pacienteName")

          // Se listan los sub-subdirectorios del subdirectorio actual, es decir, las acciones
          val acciones = paciente.listFiles()

          // Se itera sobre cada acción/sub-subdirectorios
          acciones.foreach { accion =>
            // Se comprueba que sea un directorio
            if (accion.isDirectory) {
              val accionName = accion.getName
              println(s"Procesando acción: $accionName")

              // Se definen los paths de los archivos CSV de los sensores
              val accelerometerPath = s"${accion.getAbsolutePath}/*-accelerometer-*.csv"
              val gyroscopePath = s"${accion.getAbsolutePath}/*-gyroscope-*.csv"

              // Se define el directorio base para las tablas Delta
              val baseDeltaDirectory = "D:/Archivos_uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data"

              // Se definen los paths de los directorios de las tablas Delta para el aceletómetro y el giroscopio
              val pacienteectoryDeltaPath = s"$baseDeltaDirectory/$pacienteName/$accionName"
              val accelerometerDeltaPath = s"$pacienteectoryDeltaPath/table_accelerometer.delta"
              val gyroscopeDeltaPath = s"$pacienteectoryDeltaPath/table_gyroscope.delta"

              // Se leen los ficheros CSV del acelerómetro y se escriben en las tablas Delta
              val accelerometerDF = spark.read.format("csv").option("header", true).load(accelerometerPath)
              accelerometerDF.write.format("delta").mode("overwrite").save(accelerometerDeltaPath)

              // Se leen los ficheros CSV del giroscopio y se escriben en las tablas Delta
              val gyroscopeDF = spark.read.format("csv").option("header", true).load(gyroscopePath)
              gyroscopeDF.write.format("delta").mode("overwrite").save(gyroscopeDeltaPath)

              // Se leen las tablas Delta para comprobar su funcionamiento
              val dfAccelerometerTable = spark.read.format("delta").load(accelerometerDeltaPath)
              val dfGyroscopeTable = spark.read.format("delta").load(gyroscopeDeltaPath)

              // Se muestra el contenido de la tabla Delta
              println(s"Tabla Acelerómetro ($pacienteName - $accionName):")
              dfAccelerometerTable.show()

              println(s"Tabla Giroscopio ($pacienteName - $accionName):")
              dfGyroscopeTable.show()

              // Se muestra la cantidad total de entradas de cada tabla Delta.
              println(s"Entradas en Tabla Acelerómetro ($pacienteName - $accionName): " + dfAccelerometerTable.count())
              println(s"Entradas en Tabla Giroscopio ($pacienteName - $accionName): " + dfGyroscopeTable.count())

              //  Información adicional sobre las tablas Delta
              /*
              val deltaTableAccelerometer = DeltaTable.forPath(spark, accelerometerDeltaPath)
              val deltaTableGyroscope = DeltaTable.forPath(spark, gyroscopeDeltaPath)
              val detailDFAccelerometer = deltaTableAccelerometer.detail()
              val detailDFGyroscope = deltaTableGyroscope.detail()
              detailDFAccelerometer.show()
              detailDFGyroscope.show()
              */
            }
          }
        }
      }
    } else {
      println("El directorio raíz no existe o no es un directorio.")
    }
    val endTime = System.nanoTime()
    println("Tiempo que tomó el proceso: ")
    println(((endTime - startTime) / 1000000000) + " segundos")
    spark.stop()
  }
}
