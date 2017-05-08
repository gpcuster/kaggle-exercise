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

    SparkUtils.getSpark().udf.register("convertSex", (sex: String) => sex == "male")

    SparkUtils.getSpark().udf.register("convertDouble", (age: String) => Option(age) match {
      case Some(d) => d.toDouble
      case _ => 0
    })
  }
}
