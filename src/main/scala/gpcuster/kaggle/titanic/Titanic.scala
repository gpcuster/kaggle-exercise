package gpcuster.kaggle.titanic

import gpcuster.kaggle.util.Utils

object Titanic {
  def main(args: Array[String]): Unit = {
    UDFs.registerUDFs

    val trainingDF = Utils.readCSV("src/main/resources/data/titanic/train.csv")
    val testingDF = Utils.readCSV("src/main/resources/data/titanic/test.csv")

    Visualizer.visualize(trainingDF)

    val model = Modeler.getModel(trainingDF)
    val outputDF = Predictor.getOutputDF(testingDF, model)

    Utils.generateSubmissionFile("output/titanic/", outputDF)
  }
}
