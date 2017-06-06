package gpcuster.kaggle.digit.tensorflow

import gpcuster.kaggle.util.Utils
import org.tensorflow.{Graph, Session, Tensor, TensorFlow}

object DigitRecognizer {
  def main(args: Array[String]): Unit = {
    println(TensorFlow.version())

    val g = new Graph()

    val t = Tensor.create("Hello Tensorflow Java".getBytes("UTF-8"))

    g.opBuilder("Const", "MyConst").setAttr("dtype", t.dataType()).setAttr("value", t).build()

    t.close()

    val s = new Session(g)
    val output = s.runner().fetch("MyConst").run().get(0)

    System.out.println(new String(output.bytesValue, "UTF-8"))

    output.close()
    s.close()

    g.close()

//    val trainingDF = Utils.readCSV("src/main/resources/data/digit/train.csv.gz")
//    val testingDF = Utils.readCSV("src/main/resources/data/digit/test.csv.gz")

    //    Visualizer.visualize(trainingDF)
    //
//    val model = Modeler.getModel(trainingDF)
//    val outputDF = Predictor.getOutputDF(testingDF, model)
//
//    Utils.generateSubmissionFile("output/digit/", outputDF)
  }
}
