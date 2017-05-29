package gpcuster.kaggle.digit

import breeze.linalg.DenseVector
import breeze.stats.distributions.{RandBasis, ThreadLocalRandomGenerator}
import gpcuster.kaggle.util.Utils
import keystoneml.evaluation.MulticlassClassifierEvaluator
import keystoneml.loaders.LabeledData
import keystoneml.nodes.learning.BlockLeastSquaresEstimator
import keystoneml.nodes.stats.{LinearRectifier, PaddedFFT, RandomSignNode}
import keystoneml.nodes.util.{ClassLabelIndicatorsFromIntLabels, MaxClassifier, VectorCombiner}
import keystoneml.workflow.Pipeline
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
  * This is demo using KeystoneML framework, just trying to play with it.
  */
object KeystoneMLDemo {
  def loadCSV(sc: SparkContext, path: String): RDD[DenseVector[Double]] = {
    sc.textFile(path).filter(str => str(0).isDigit).map(row => DenseVector(row.split(",").map(_.toDouble)))
  }

  def run(conf: MnistRandomFFTConfig): Unit = {
    // This is a property of the MNIST Dataset (digits 0 - 9)
    val numClasses = 10

    val randomSignSource = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(conf.seed)))

    // The number of pixels in an MNIST image (28 x 28 = 784)
    // Because the mnistImageSize is 784, we get 512 PaddedFFT features per FFT.
    val mnistImageSize = 784

    val train = LabeledData(
      loadCSV(Utils.getSpark.sparkContext, conf.trainLocation)
        .map(x => (x(0).toInt, x(1 until x.length)))
        .cache())
    val labels = ClassLabelIndicatorsFromIntLabels(numClasses).apply(train.labels)

    val featurizer = Pipeline.gather {
      Seq.fill(conf.numFFTs) {
        RandomSignNode(mnistImageSize, randomSignSource) andThen PaddedFFT() andThen LinearRectifier(0.0)
      }
    } andThen VectorCombiner()

    val pipeline = featurizer andThen
      (new BlockLeastSquaresEstimator(conf.blockSize, 1, conf.lambda.getOrElse(0)),
        train.data, labels) andThen
      MaxClassifier

    // Calculate train error
    val evaluator = new MulticlassClassifierEvaluator(numClasses)
    val trainEval = evaluator.evaluate(pipeline(train.data), train.labels)
    println("TRAIN Error is " + (100 * trainEval.totalError) + "%")

    val schema = new StructType(Array(StructField("ImageId",IntegerType,nullable = false),StructField("Label",IntegerType,nullable = false)))
    val outputDF = Utils.getSpark.createDataFrame(pipeline(loadCSV(Utils.getSpark.sparkContext, conf.testLocation)).get.mapPartitions(it => {
        // this works because there is only 1 partition.
        var imageId:Int = 0
        it.map( label => {
          imageId += 1
          Row(imageId, label)
        })
    }), schema)

    Utils.generateSubmissionFile("output/digit/", outputDF)
  }

  case class MnistRandomFFTConfig(
                                   trainLocation: String = "src/main/resources/data/digit/train.csv.gz",
                                   testLocation: String = "src/main/resources/data/digit/test.csv.gz",
                                   numFFTs: Int = 4,
                                   blockSize: Int = 2048,
                                   numPartitions: Int = 10,
                                   lambda: Option[Double] = None,
                                   seed: Long = 0)

  def main(args: Array[String]) = {
    val appConfig = MnistRandomFFTConfig()

    run(appConfig)
  }
}
