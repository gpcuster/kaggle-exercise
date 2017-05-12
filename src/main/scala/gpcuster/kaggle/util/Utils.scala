package gpcuster.kaggle.util

import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{OneHotEncoder, SQLTransformer, StringIndexer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object Utils {
  GlobalUDFs.registerUDFs

  lazy val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")

  val OneHotEncoderFieldSuffix = "_OHE"
  val IntConverterFieldSuffix = "_INT"

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
    Utils.getSpark().read.format("com.databricks.spark.csv")
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

    Utils.writeCSV(outputPath, df)
  }

  def oneHotEncoding(fieldName: String) = {
    val encodedFiledName = fieldName + OneHotEncoderFieldSuffix
    val si = new StringIndexer().setHandleInvalid("error").setInputCol(fieldName).setOutputCol(fieldName + "SI")
    val oh = new OneHotEncoder().setInputCol(fieldName + "SI").setOutputCol(encodedFiledName)

    (encodedFiledName, Array(si, oh))
  }

  def convertNumberToInt(fieldName: String, default: Int = 0) = {
    val encodedFiledName = fieldName + IntConverterFieldSuffix

    val sqlTrans = new SQLTransformer().setStatement(
      s"""SELECT *, convertNumberToIntOrZero($fieldName) AS $encodedFiledName FROM __THIS__""")

    (encodedFiledName, Array(sqlTrans))
  }
}
