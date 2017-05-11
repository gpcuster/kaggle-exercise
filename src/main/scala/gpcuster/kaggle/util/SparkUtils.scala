package gpcuster.kaggle.util

import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object SparkUtils {
  lazy val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")

  def getSpark() = {
    val conf = new SparkConf().setMaster("local[2]")
    val builder = SparkSession.builder.config(conf)
    val spark = builder.getOrCreate()
    spark
  }

  def sql(sql: String) = {
    getSpark().sql(sql)
  }

  def sqlPrint(sql: String) = {
    getSpark().sql(sql).collect().foreach(println)
  }

  def readCSV(path: String, schema: StructType = null) = {
    SparkUtils.getSpark().read.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .schema(schema)
      .load(path)
  }

  def writeCSV(path: String, df: DataFrame) = {
    df.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(path)
  }

  def generateSubmissionFile(path: String, df: DataFrame) = {
    val runId:String = sdf.format(System.currentTimeMillis())

    val outputPath = if (path.endsWith("/")) {
      path + runId
    } else {
      path + "/" + runId
    }

    SparkUtils.writeCSV(outputPath, df)
  }
}
