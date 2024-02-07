package org.tfmupm

import org.apache.spark.sql.SparkSession
import io.delta.tables._
import java.io.File

object SparkRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Reading from csv tables")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Define the parent directory
    val parentDirectory = new File("D:/Archivos uni/TFM/dataset/data/ADL_sequences")

    // Check if the parent directory exists and is a directory
    if (parentDirectory.exists() && parentDirectory.isDirectory) {
      // List the contents of the parent directory
      val subdirectories = parentDirectory.listFiles()

      // Iterate over each file/subdirectory in the parent directory
      subdirectories.foreach { subdir =>
        // Check if it's a directory
        if (subdir.isDirectory) {
          val subdirName = subdir.getName
          println(s"Processing directory: $subdirName")

          // List the contents of the subdirectory
          val subsubdirectories = subdir.listFiles()

          // Iterate over each sub-subdirectory
          subsubdirectories.foreach { subsubdir =>
            // Check if it's a directory
            if (subsubdir.isDirectory) {
              val subsubdirName = subsubdir.getName
              println(s"Processing subdirectory: $subsubdirName")

              // Define paths to the CSV files for accelerometer and gyroscope data in the current sub-subdirectory
              val accelerometerPath = s"${subsubdir.getAbsolutePath}/*-accelerometer-*.csv"
              val gyroscopePath = s"${subsubdir.getAbsolutePath}/*-gyroscope-*.csv"

              // Define the base directory where the Delta tables will be saved
              val baseDeltaDirectory = "D:/Archivos uni/TFM/TFMDataLake/src/main/scala/org/tfmupm/data"

              // Define paths for Delta tables for accelerometer and gyroscope data in the current sub-subdirectory
              val subdirectoryDeltaPath = s"$baseDeltaDirectory/$subdirName/$subsubdirName"
              val accelerometerDeltaPath = s"$subdirectoryDeltaPath/table_accelerometer.delta"
              val gyroscopeDeltaPath = s"$subdirectoryDeltaPath/table_gyroscope.delta"

              // Read accelerometer data from CSV and write to Delta table
              val accelerometerDF = spark.read.format("csv").option("header", true).load(accelerometerPath)
              accelerometerDF.write.format("delta").mode("overwrite").save(accelerometerDeltaPath)

              // Read gyroscope data from CSV and write to Delta table
              val gyroscopeDF = spark.read.format("csv").option("header", true).load(gyroscopePath)
              gyroscopeDF.write.format("delta").mode("overwrite").save(gyroscopeDeltaPath)

              // Read the Delta tables back for verification or further processing if needed
              val dfAccelerometerTable = spark.read.format("delta").load(accelerometerDeltaPath)
              val dfGyroscopeTable = spark.read.format("delta").load(gyroscopeDeltaPath)

              // Show content of the Delta tables
              println(s"Accelerometer Table ($subdirName - $subsubdirName):")
              dfAccelerometerTable.show()

              println(s"Gyroscope Table ($subdirName - $subsubdirName):")
              dfGyroscopeTable.show()

              // Print counts of entries in the Delta tables
              println(s"Accelerometer Table Count ($subdirName - $subsubdirName): " + dfAccelerometerTable.count())
              println(s"Gyroscope Table Count ($subdirName - $subsubdirName): " + dfGyroscopeTable.count())

              // Additional checks can be performed using DeltaTable's API if needed
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
      println("The specified directory does not exist or is not a directory.")
    }

    spark.stop()
  }
}
