package gpcuster.kaggle.houseprice

import gpcuster.kaggle.util.SparkUtils
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{OneHotEncoder, SQLTransformer, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Estimator, Pipeline, PipelineStage, Transformer}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

object Modeler {
  def getModel(inputDF: DataFrame): Transformer = {
    val labelCol = "SalePrice"

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

    var encodedFieldNames = Array[String]()
    var oneHotEncoders = Array[PipelineStage]()
    val oneHotEncodingFields = Array(
      "MSSubClass"
      ,"MSZoning"
      ,"Street"
      ,"LotShape"
      ,"LandContour"
      ,"Utilities"
      ,"LotConfig"
      ,"LandSlope"
      ,"Neighborhood"
      ,"Condition1" // Condition2
      ,"BldgType"
      ,"HouseStyle"
      ,"OverallQual"
      ,"OverallCond"
      ,"RoofStyle"
      ,"RoofMatl"
      ,"Exterior1st" // Exterior2nd
      ,"MasVnrType"
      ,"ExterQual"
      ,"ExterCond"
      ,"Foundation"
      ,"BsmtQual"
      ,"BsmtCond"
      ,"BsmtExposure"
      ,"BsmtFinType1" // BsmtFinType2
      ,"Heating"
      ,"HeatingQC"
      ,"CentralAir"
      ,"Electrical"
      ,"KitchenQual"
      ,"Functional"
      ,"FireplaceQu"
//      ,"GarageType"
//      ,"GarageFinish"
//      ,"GarageQual"
//      ,"GarageCond"
//      ,"PavedDrive"
//      ,"PoolQC"
      ,"Fence"
      ,"MiscFeature"
      ,"SaleType"
      ,"SaleCondition"
    )
    for (fieldName <- oneHotEncodingFields) {
      val (encodedFieldName, encoder) = SparkUtils.oneHotEncoding(fieldName)

      encodedFieldNames = encodedFieldNames ++ Array(encodedFieldName)
      oneHotEncoders = oneHotEncoders ++ encoder
    }

    val assembler = new VectorAssembler()
      .setInputCols(encodedFieldNames ++ Array(
        "LotArea"
        ,"YearBuilt"
        ,"MoSold"
        ,"YrSold"
        ,"MiscVal"
      ))
      .setOutputCol("features")

    val sqlTransSkipMockupData = new SQLTransformer().setStatement(
      "SELECT * FROM __THIS__ where Id > 0")

    val alg = "lr"

    val estimator = if (alg == "lr") {
      new LinearRegression()
        .setMaxIter(150)
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
      .setStages(oneHotEncoders ++ Array(sqlTransSkipMockupData, assembler, trainingEstimator))

    val pipelineModel = pipeline.fit(training)

    val prediction = pipelineModel.transform(test)

    prediction.show

    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val trainingRMSE = evaluator.evaluate(pipelineModel.transform(training))
    println("Training Data Set Root-Mean-Squared-Error: " + trainingRMSE)

    val testRMSE = evaluator.evaluate(prediction)
    println("Test Data Set Root-Mean-Squared-Error: " + testRMSE)

    pipelineModel
  }
}
