package peng.kaggle.titanic

import org.apache.spark.sql.types._
import peng.kaggle.util.SparkUtils
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.ml.classification.{LogisticRegression, DecisionTreeClassifier, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.{Estimator, Transformer}

object App {
  def main(args: Array[String]): Unit = {
    val inputPath = "src/main/resources/data/titanic/train.csv"

    //guessed,difficultyLevel,confidenceAverage,choiceChanged,differentChoices,underExpectedTime
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

    val convertSex = udf { sex: String => sex == "male"}

    val convertAge = udf {
      age: String => Option(age) match {
      case Some(d) => d.toDouble
      case _ => 0
      }
    }

    val convertedInpuDF = inputDF.withColumn("Sex2", convertSex(col("Sex"))).withColumn("Age2", convertAge(col("Age")))

    convertedInpuDF.show

    val Array(training, test) = convertedInpuDF.randomSplit(Array(0.7, 0.3), seed = 12345)


    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "Pclass",
        //"Sex",
        "Age2",
        "SibSp",
        "Parch",
        //"Ticket",
        "Fare"
        //"Cabin",
        //"Embarked"
      ))
      .setOutputCol("features")

    val trainingWithFeatures = assembler.transform(training)
    val testWithFeatures = assembler.transform(test)

    trainingWithFeatures.show(10, false)

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

    val model = estimator.asInstanceOf[Estimator[_]].fit(trainingWithFeatures).asInstanceOf[Transformer]
  }
}
