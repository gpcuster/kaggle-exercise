package gpcuster.kaggle.houseprice

import gpcuster.kaggle.util.Utils

object UDFs {
  def registerUDFs = {
    Utils.getSpark().udf.register("extractTitle", (name: String) => {
      val startIndex = name.indexOf(", ")
      val endIndex = name.indexOf(". ", startIndex)

      name.substring(startIndex + 2, endIndex)
    })

    Utils.getSpark().udf.register("convertEmbarked", (embarked: String) =>  Option(embarked) match {
      case Some(s) if s == "S" => 1
      case Some(s) if s == "C" => 2
      case Some(s) if s == "Q" => 3
      case _ => 1
    })

    Utils.getSpark().udf.register("pClass1", (pClass: String) => pClass == "1")
    Utils.getSpark().udf.register("pClass2", (pClass: String) => pClass == "2")

    Utils.getSpark().udf.register("embarkedC", (embarked: String) => embarked == "C")
    Utils.getSpark().udf.register("embarkedQ", (embarked: String) => embarked == "Q")

    Utils.getSpark().udf.register("hasFamily", (SibSp: Int, Parch: Int) => SibSp + Parch > 0)

    Utils.getSpark().udf.register("child", (age: Int) => age < 16)
    Utils.getSpark().udf.register("male", (sex: String, age: Int) => age>= 16 && sex == "male")
    Utils.getSpark().udf.register("female", (sex: String, age: Int) => age>= 16 && sex == "female")

    Utils.getSpark().udf.register("convertDouble", (age: String) => Option(age) match {
      case Some(d) => d.toDouble.toInt
      case _ => 0
    })
  }
}
