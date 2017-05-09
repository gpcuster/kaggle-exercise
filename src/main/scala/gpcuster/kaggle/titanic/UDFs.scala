package gpcuster.kaggle.titanic

import gpcuster.kaggle.util.SparkUtils

object UDFs {
  def registerUDFs = {
    SparkUtils.getSpark().udf.register("extractTitle", (name: String) => {
      val startIndex = name.indexOf(", ")
      val endIndex = name.indexOf(". ", startIndex)

      name.substring(startIndex + 2, endIndex)
    })

    SparkUtils.getSpark().udf.register("convertEmbarked", (embarked: String) =>  Option(embarked) match {
      case Some(s) if s == "S" => 1
      case Some(s) if s == "C" => 2
      case Some(s) if s == "Q" => 3
      case _ => 1
    })

    SparkUtils.getSpark().udf.register("pClass1", (pClass: String) => pClass == "1")
    SparkUtils.getSpark().udf.register("pClass2", (pClass: String) => pClass == "2")

    SparkUtils.getSpark().udf.register("embarkedC", (embarked: String) => embarked == "C")
    SparkUtils.getSpark().udf.register("embarkedQ", (embarked: String) => embarked == "Q")

    SparkUtils.getSpark().udf.register("hasFamily", (SibSp: Int, Parch: Int) => SibSp + Parch > 0)

    SparkUtils.getSpark().udf.register("child", (age: Int) => age < 16)
    SparkUtils.getSpark().udf.register("male", (sex: String, age: Int) => age>= 16 && sex == "male")
    SparkUtils.getSpark().udf.register("female", (sex: String, age: Int) => age>= 16 && sex == "female")

    SparkUtils.getSpark().udf.register("convertDouble", (age: String) => Option(age) match {
      case Some(d) => d.toDouble.toInt
      case _ => 0
    })
  }
}
