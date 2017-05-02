package gpcuster.kaggle.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object SparkUtils {
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

  def readCSV(path: String, schema: StructType) = {
    val df = SparkUtils.getSpark().read.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .schema(schema)
      .load(path)

    df
  }

  def writeCSV(path: String, df: DataFrame) = {
    df.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(path)
  }

}
