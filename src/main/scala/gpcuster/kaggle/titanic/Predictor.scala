package gpcuster.kaggle.titanic

import gpcuster.kaggle.util.SparkUtils
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Predictor {
  def getOutputDF(model: Transformer): DataFrame = {
    val inputPath = "src/main/resources/data/titanic/test.csv"

    val customSchema = StructType(Array(
      StructField("PassengerId", IntegerType, false),
      //StructField("Survived", IntegerType, false),
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

    val convertSex = udf { sex: String => sex == "male"}

    val convertAge = udf {
      age: String => Option(age) match {
        case Some(d) => d.toDouble
        case _ => 0
      }
    }

    val convertedInpuDF = inputDF
      .withColumn("Sex2", convertSex(col("Sex")))
      .withColumn("Age2", convertAge(col("Age")))
      .withColumn("Fare2", convertAge(col("Fare")))

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

    val inputWithFeatures = assembler.transform(convertedInpuDF)

    val prediction = model.transform(inputWithFeatures)

    prediction.createOrReplaceTempView("outputTable")

    val convertPrediction = udf {
      prediction: Double => prediction match {
        case  survived if survived > 0 => 1
        case _ => 0
      }
    }

    val outputDF = prediction.withColumn("Survived", convertPrediction(col("prediction"))).select("PassengerId", "Survived")

    outputDF
  }
}
