package gpcuster.kaggle.digit

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql.DataFrame

object Modeler {
  def getModel(inputDF: DataFrame): Transformer = {
    val labelCol = "label"

    val pixelCols = Array.range(0, 784).map(index => "pixel"+ index)
    val assembler = new VectorAssembler()
      .setInputCols(pixelCols)
      .setOutputCol("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")

    val rf = new RandomForestClassifier()
        .setLabelCol(labelCol)
        .setFeaturesCol("pcaFeatures")
        .setMaxMemoryInMB(512)

    val pipeline = new Pipeline()
      .setStages(Array(assembler, pca, rf))

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val paramGrid = new ParamGridBuilder()
      .addGrid(pca.k, Array(100, 200))
      .addGrid(rf.numTrees, Array(80, 160))
      .addGrid(rf.maxDepth, Array(10, 20))
      .addGrid(rf.maxBins, Array(32, 64))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setSeed(12345)
      .setNumFolds(3)

    val pipelineModel = cv.fit(inputDF)

    pipelineModel
  }
}
