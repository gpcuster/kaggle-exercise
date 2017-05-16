package gpcuster.kaggle.houseprice

import gpcuster.kaggle.util.Utils
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{SQLTransformer, VectorAssembler}
import org.apache.spark.ml.regression._
import org.apache.spark.ml.{Estimator, Pipeline, PipelineStage, Transformer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, log}

object Modeler {
  def getModel(inputDF: DataFrame): Transformer = {
    val labelCol = "SalePriceLog"

    val inputDFWithLogPrice = inputDF.withColumn(labelCol, log(col("SalePrice") + 1))

    val Array(training, test) = inputDFWithLogPrice.randomSplit(Array(0.7, 0.3), seed = 12345)

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
      ,"Condition1", "Condition2"
      ,"BldgType"
      ,"HouseStyle"
      ,"OverallQual"
      ,"OverallCond"
      ,"RoofStyle"
      ,"RoofMatl"
      ,"Exterior1st", "Exterior2nd"
      ,"MasVnrType"
      ,"ExterQual"
      ,"ExterCond"
      ,"Foundation"
      ,"BsmtQual"
      ,"BsmtCond"
      ,"BsmtExposure"
      ,"BsmtFinType1", "BsmtFinType2"
      ,"Heating"
      ,"HeatingQC"
      ,"CentralAir"
      ,"Electrical"
      ,"KitchenQual"
      ,"Functional"
      ,"FireplaceQu"
      ,"GarageType"
      ,"GarageFinish"
      ,"GarageQual"
      ,"GarageCond"
      ,"PavedDrive"
      ,"PoolQC"
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

    val alg = "rf"

    val estimator = if (alg == "lr") {
      new LinearRegression()
        .setMaxIter(200)
        .setRegParam(0.0001)
        .setElasticNetParam(0.5)
        .setAggregationDepth(5)
        .setLabelCol(labelCol)
        .setFeaturesCol("features")
    } else if (alg == "glr") {
      new GeneralizedLinearRegression()
        .setFamily("gaussian")
        .setLink("identity")
        .setMaxIter(200)
        .setRegParam(0.0001)
        .setLabelCol(labelCol)
        .setFeaturesCol("features")
    } else if (alg == "dt") {
      new DecisionTreeRegressor()
        .setLabelCol(labelCol)
        .setFeaturesCol("features")
    } else if (alg == "rf") {
      new RandomForestRegressor()
        .setLabelCol(labelCol)
        .setFeaturesCol("features")
        .setNumTrees(50)
        .setMaxBins(100)
        .setMaxDepth(20)
    } else if (alg == "gbt") {
      new GBTRegressor()
        .setLabelCol(labelCol)
        .setFeaturesCol("features")
    }

    val trainingEstimator = estimator.asInstanceOf[Estimator[_]]

    val pipeline = new Pipeline()
      .setStages(oneHotEncoders ++ intTransformers ++ Array(sqlTransSkipMockupData, sqlTrans, assembler, trainingEstimator))

    val pipelineModel = pipeline.fit(inputDFWithLogPrice)

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
