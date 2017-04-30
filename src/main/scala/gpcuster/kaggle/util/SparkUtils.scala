package gpcuster.kaggle.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def getSpark() = {
    val conf = new SparkConf().setMaster("local[2]")
    val builder = SparkSession.builder.config(conf)
    val spark = builder.getOrCreate()
    spark
  }

  def sql(sql: String) = {
    getSpark().sql(sql).collect().foreach(println)
  }
}
