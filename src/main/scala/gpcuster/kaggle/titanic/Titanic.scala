package gpcuster.kaggle.titanic

import gpcuster.kaggle.util.SparkUtils

object Titanic {
  def main(args: Array[String]): Unit = {
    UDFs.registerUDFs

    val trainingDF = SparkUtils.readCSV("src/main/resources/data/titanic/train.csv")
    val testingDF = SparkUtils.readCSV("src/main/resources/data/titanic/test.csv")

    Visualizer.visualize(trainingDF)

    val model = Modeler.getModel(trainingDF)
    val outputDF = Predictor.getOutputDF(testingDF, model)

    SparkUtils.generateSubmissionFile("output/titanic/", outputDF)
  }
}
