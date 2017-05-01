package gpcuster.kaggle.titanic

import org.apache.spark.ml.Transformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Predictor {
  def getOutputDF(inputDF: DataFrame, model: Transformer): DataFrame = {
    val prediction = model.transform(inputDF)

    prediction.createOrReplaceTempView("outputTable")

    val convertPrediction = udf {
      prediction: Double => prediction match {
        case  survived if survived > 0 => 1
        case _ => 0
      }
    }

    val outputDF = prediction.withColumn("Survived", convertPrediction(col("prediction"))).select("PassengerId", "Survived")

    outputDF
  }
}
