package gpcuster.kaggle.digit.tensorflow

import gpcuster.kaggle.util.Utils

object DigitRecognizer {
  def main(args: Array[String]): Unit = {
    val trainingDF = Utils.readCSV("src/main/resources/data/digit/train.csv.gz")
    val testingDF = Utils.readCSV("src/main/resources/data/digit/test.csv.gz")

    //    Visualizer.visualize(trainingDF)
    //
//    val model = Modeler.getModel(trainingDF)
//    val outputDF = Predictor.getOutputDF(testingDF, model)
//
//    Utils.generateSubmissionFile("output/digit/", outputDF)
  }
}
