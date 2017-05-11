package gpcuster.kaggle.titanic

import java.text.SimpleDateFormat

import gpcuster.kaggle.util.SparkUtils

object App {
  def main(args: Array[String]): Unit = {

    UDFs.registerUDFs

    val trainingPath = "src/main/resources/data/titanic/train.csv"
    val trainingDF = SparkUtils.readCSV(trainingPath)

    val testingPath = "src/main/resources/data/titanic/test.csv"
    val testingDF = SparkUtils.readCSV(testingPath)

    Visualizer.visualize(trainingDF)

    val model = Modeler.getModel(trainingDF)
    val outputDF = Predictor.getOutputDF(testingDF, model)

    val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val runId:String = df.format(System.currentTimeMillis())

    SparkUtils.writeCSV("output/titanic/" + runId, outputDF)
  }
}
