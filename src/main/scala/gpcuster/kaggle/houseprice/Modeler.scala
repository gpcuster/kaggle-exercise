package gpcuster.kaggle.houseprice

import gpcuster.kaggle.util.Utils
import org.apache.spark.ml.classification.{DecisionTreeClassifier, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{SQLTransformer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Estimator, Pipeline, PipelineStage, Transformer}
import org.apache.spark.sql.DataFrame

object Modeler {
  def getModel(inputDF: DataFrame): Transformer = {
    val labelCol = "SalePrice"

    val Array(training, test) = inputDF.randomSplit(Array(0.7, 0.3), seed = 12345)

    var encodedFieldNames = Array[String]()
    var oneHotEncoders = Array[PipelineStage]()
    var intFieldNames = Array[String]()
    var intTransformers = Array[PipelineStage]()

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
      ,"GarageType"
      ,"GarageFinish"
//      ,"GarageQual"
//      ,"GarageCond"
      ,"PavedDrive"
//      ,"PoolQC"
      ,"Fence"
      ,"MiscFeature"
      ,"SaleType"
      ,"SaleCondition"
    )

    for (fieldName <- oneHotEncodingFields) {
      val (encodedFieldName, encoder) = Utils.oneHotEncoding(fieldName)

      encodedFieldNames = encodedFieldNames ++ Array(encodedFieldName)
      oneHotEncoders = oneHotEncoders ++ encoder
    }

    val numberFields = Array(
      "LotFrontage"
      ,"LotArea"
      ,"YearBuilt"
      ,"MasVnrArea"
      ,"BsmtFinSF1"
      ,"BsmtFinSF2"
      ,"BsmtUnfSF"
      ,"TotalBsmtSF"
      ,"1stFlrSF"
      ,"2ndFlrSF"
      ,"LowQualFinSF"
      ,"GrLivArea"
      ,"BsmtFullBath"
      ,"BsmtHalfBath"
      ,"FullBath"
      ,"HalfBath"
      ,"BedroomAbvGr"
      ,"KitchenAbvGr"
      ,"TotRmsAbvGrd"
      ,"Fireplaces"
      ,"GarageYrBlt"
      ,"GarageCars"
      ,"GarageArea"
      ,"WoodDeckSF"
      ,"OpenPorchSF"
      ,"EnclosedPorch"
      ,"3SsnPorch"
      ,"ScreenPorch"
      ,"PoolArea"
      ,"MoSold"
      ,"YrSold"
      ,"MiscVal"
    )

    for (fieldName <- numberFields) {
      val (convertedFieldName, encoder) = Utils.convertNumberToInt(fieldName)

      intFieldNames = intFieldNames ++ Array(convertedFieldName)
      intTransformers = intTransformers ++ encoder
    }

    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT * " +
        ", (YearRemodAdd - YearBuilt) AS RemodAdd " +
        "FROM __THIS__")

    val assembler = new VectorAssembler()
      .setInputCols(encodedFieldNames ++ intFieldNames ++ Array(
        "RemodAdd"
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
      .setStages(oneHotEncoders ++ intTransformers ++ Array(sqlTransSkipMockupData, sqlTrans, assembler, trainingEstimator))

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

    val negativePredictionCount = prediction.where("prediction <= 0").count()
    println("Test Data Set Negative Prediction Count: " + negativePredictionCount)

    pipelineModel
  }
}
