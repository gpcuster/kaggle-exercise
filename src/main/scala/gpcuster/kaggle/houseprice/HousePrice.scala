package gpcuster.kaggle.houseprice

import gpcuster.kaggle.titanic.UDFs
import gpcuster.kaggle.util.SparkUtils

object HousePrice {
  def main(args: Array[String]): Unit = {
    UDFs.registerUDFs

    val trainingDF = SparkUtils.readCSV("src/main/resources/data/house_price/train.csv")
    val testingDF = SparkUtils.readCSV("src/main/resources/data/house_price/test.csv")

//    Visualizer.visualize(trainingDF)

    val model = Modeler.getModel(trainingDF)
    val outputDF = Predictor.getOutputDF(testingDF, model)

    SparkUtils.generateSubmissionFile("output/hourse_price/", outputDF)
  }
}
