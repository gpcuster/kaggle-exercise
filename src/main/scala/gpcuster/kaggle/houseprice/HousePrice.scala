package gpcuster.kaggle.houseprice

import gpcuster.kaggle.util.{GlobalUDFs, Utils}

object HousePrice {
  def main(args: Array[String]): Unit = {
    GlobalUDFs.registerUDFs
    UDFs.registerUDFs

    val trainingDF = Utils.readCSV("src/main/resources/data/house_price/train.csv")
    val testingDF = Utils.readCSV("src/main/resources/data/house_price/test.csv")

//    Visualizer.visualize(trainingDF)
//
    val model = Modeler.getModel(trainingDF)
    val outputDF = Predictor.getOutputDF(testingDF, model)

    Utils.generateSubmissionFile("output/hourse_price/", outputDF)
  }
}
