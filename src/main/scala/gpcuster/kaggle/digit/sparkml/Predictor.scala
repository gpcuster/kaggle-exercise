package gpcuster.kaggle.digit.sparkml

import gpcuster.kaggle.util.Utils
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

object Predictor {
  def getOutputDF(inputDF: DataFrame, model: Transformer): DataFrame = {
    val prediction = model.transform(inputDF)

    val schema = new StructType(Array(StructField("ImageId",IntegerType,nullable = false),StructField("Label",IntegerType,nullable = false)))
    val outputDF = Utils.getSpark.createDataFrame(prediction.rdd.mapPartitions(iterator => {
      // this works because there is only 1 partition.
      var imageId:Int = 0
      iterator.map( row => {
        imageId += 1
        Row(imageId, row.getAs[Double]("prediction").toInt)
      })
    }), schema)

    outputDF
  }
}
