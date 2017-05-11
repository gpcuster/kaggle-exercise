package gpcuster.kaggle.houseprice

import gpcuster.kaggle.util.SparkUtils

object App {
  def main(args: Array[String]): Unit = {
    val trainingPath = "src/main/resources/data/house_price/train.csv"

    val trainingDF = SparkUtils.readCSV(trainingPath)

    trainingDF.show
  }
}
