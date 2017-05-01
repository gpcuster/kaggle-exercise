package gpcuster.kaggle.titanic

import gpcuster.kaggle.util.SparkUtils
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{SQLTransformer, VectorAssembler}
import org.apache.spark.ml.{Estimator, Pipeline, Transformer}
import org.apache.spark.sql.types._

object Modeler {
  def getModel(): Transformer = {
    val inputPath = "src/main/resources/data/titanic/train.csv"

    val customSchema = StructType(Array(
      StructField("PassengerId", IntegerType, false),
      StructField("Survived", IntegerType, false),
      StructField("Pclass", IntegerType, false),
      StructField("Name", StringType, false),
      StructField("Sex", StringType, false),
      StructField("Age", DoubleType, true),
      StructField("SibSp", IntegerType, false),
      StructField("Parch", IntegerType, false),
      StructField("Ticket", StringType, false),
      StructField("Fare", DoubleType, false),
      StructField("Cabin", StringType, false),
      StructField("Embarked", StringType, false)
    )
    )

    val inputDF = SparkUtils.getSpark().read.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .schema(customSchema)
      .load(inputPath)

    inputDF.createOrReplaceTempView("inputTable")

    inputDF.show(10, false)

    SparkUtils.sql("select * from inputTable where age is null")

    SparkUtils.sql("select * from inputTable where survived = 1")

    SparkUtils.getSpark().udf.register("convertSex", (sex: String) => sex == "male")
    SparkUtils.getSpark().udf.register("convertDouble", (age: String) => Option(age) match {
      case Some(d) => d.toDouble
      case _ => 0
    })

    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, convertSex(Sex) AS Sex2, convertDouble(Age) AS Age2, convertDouble(Fare) AS Fare2 FROM __THIS__")

    val Array(training, test) = inputDF.randomSplit(Array(0.7, 0.3), seed = 12345)

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "Pclass",
        "Sex2",
        "Age2",
        "SibSp",
        "Parch",
        //"Ticket",
        "Fare2"
        //"Cabin",
        //"Embarked"
      ))
      .setOutputCol("features")

    val alg = "lr"

    val estimator = if (alg == "lr") {
      new LogisticRegression()
        .setMaxIter(100)
        .setRegParam(0.000001)
        .setLabelCol("Survived")
        .setFeaturesCol("features")
    } else if (alg == "dt") {
      new DecisionTreeClassifier()
        .setLabelCol("Survived")
        .setFeaturesCol("features")
    } else if (alg == "rf") {
      new RandomForestClassifier()
        .setLabelCol("Survived")
        .setFeaturesCol("features")
        .setNumTrees(10)
    } else if (alg == "nb") {
      new NaiveBayes()
        .setLabelCol("Survived")
        .setFeaturesCol("features")
    }

    val trainingEstimator = estimator.asInstanceOf[Estimator[_]]

    val pipeline = new Pipeline()
      .setStages(Array(sqlTrans, assembler, trainingEstimator))

    val pipelineModel = pipeline.fit(training)

    val prediction = pipelineModel.transform(test)

    prediction.show(100, false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("Survived")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val trainingAccuracy = evaluator.evaluate(pipelineModel.transform(training))
    println("Training Data Set Accuracy: " + trainingAccuracy)

    val testAccuracy = evaluator.evaluate(prediction)
    println("Test Data Set Accuracy: " + testAccuracy)

    pipelineModel
  }
}
