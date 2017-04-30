package gpcuster.kaggle.titanic

import java.text.SimpleDateFormat

object App {
  def main(args: Array[String]): Unit = {
    val model = Modeler.getModel()
    val outputDF = Predictor.getOutputDF(model)

    val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss")
    val date:String = df.format(System.currentTimeMillis())

    outputDF.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("output/titanic/" + date)
  }
}
