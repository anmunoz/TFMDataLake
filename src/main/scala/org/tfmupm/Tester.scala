package org.tfmupm
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import io.delta.tables._
import java.io.File

object Tester {
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

    println("Appname: " + spark.sparkContext.appName)
    val baseDeltaDirectory = "D:/Archivos uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data"
    val paciente = "944363"
    val accion = "ef"
    val dfGyroscopeTable = spark.read.format("delta").load(baseDeltaDirectory + "/" + paciente + "/" + accion + "/table_gyroscope.delta")
    // Para obtener el valor total de una columna por ejemplo 'x'
    val sumX = dfGyroscopeTable.agg(sum("x")).collect()(0)(0).asInstanceOf[Double]
    // Show the values of the 'x' column
    println("Valor total de la columna 'x'")
    println(sumX)
    val endTime = System.nanoTime()
    println("Tiempo que tomó el cálculo")
    println(((endTime - startTime) / 1000000000) + " segundos")
    spark.stop()
  }
}
