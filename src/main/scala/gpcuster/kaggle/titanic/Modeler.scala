package gpcuster.kaggle.titanic

import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{SQLTransformer, VectorAssembler}
import org.apache.spark.ml.{Estimator, Pipeline, Transformer}
import org.apache.spark.sql.DataFrame

object Modeler {
  def getModel(inputDF: DataFrame): Transformer = {
    val labelCol = "Survived"

    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, " +
        "child(convertDouble(Age)) AS child, " +
        "male(Sex, convertDouble(Age)) AS male, " +
        "female(Sex, convertDouble(Age)) AS female, " +
        "convertDouble(Age) AS Age2, " +
        "convertDouble(Fare) AS Fare2, " +
        "hasFamily(SibSp, Parch) AS hasFamily, " +
        "pClass1(Pclass) AS pClass1, " +
        "pClass2(Pclass) AS pClass2, " +
        "embarkedQ(Embarked) AS embarkedQ, " +
        "embarkedC(Embarked) AS embarkedC " +
        "FROM __THIS__")

    val Array(training, test) = inputDF.randomSplit(Array(0.7, 0.3), seed = 12345)

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "pClass1",
        "pClass2",
        "child",
        "male",
        "female",
        "Age2",
        "hasFamily",
        //"Ticket",
        "Fare2"
        //"Cabin",
        ,"embarkedQ"
        ,"embarkedC"
      ))
      .setOutputCol("features")

    val alg = "rf"//"lr"

    val estimator = if (alg == "lr") {
      new LogisticRegression()
        .setMaxIter(100)
        .setRegParam(0.000001)
        .setLabelCol(labelCol)
        .setFeaturesCol("features")
    } else if (alg == "dt") {
      new DecisionTreeClassifier()
        .setLabelCol(labelCol)
        .setFeaturesCol("features")
    } else if (alg == "rf") {
      new RandomForestClassifier()
        .setLabelCol(labelCol)
        .setFeaturesCol("features")
        .setNumTrees(20)
    } else if (alg == "nb") {
      new NaiveBayes()
        .setLabelCol(labelCol)
        .setFeaturesCol("features")
    }

    val trainingEstimator = estimator.asInstanceOf[Estimator[_]]

    val pipeline = new Pipeline()
      .setStages(Array(sqlTrans, assembler, trainingEstimator))

    val pipelineModel = pipeline.fit(training)

    val prediction = pipelineModel.transform(test)

    prediction.show(100, false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val trainingAccuracy = evaluator.evaluate(pipelineModel.transform(training))
    println("Training Data Set Accuracy: " + trainingAccuracy)

    val testAccuracy = evaluator.evaluate(prediction)
    println("Test Data Set Accuracy: " + testAccuracy)

    pipelineModel
  }
}
