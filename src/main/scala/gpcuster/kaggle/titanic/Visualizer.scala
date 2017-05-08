package gpcuster.kaggle.titanic

import gpcuster.kaggle.util.SparkUtils
import org.apache.spark.sql.DataFrame
import vegas._
import vegas.sparkExt._

object Visualizer {
  def visualize(inputDF: DataFrame) = {

    inputDF.createOrReplaceTempView("inputTable")

    inputDF.show(10, false)

    inputDF.describe().show()

    // Pclass
    Vegas("A simple bar chart showing relation bettween Pclass and SurvivedRate.").
      withDataFrame(SparkUtils.sql("select Pclass, avg(Survived) as SurvivedRate from inputTable group by Pclass")).
      encodeX("Pclass", Nominal).
      encodeY("SurvivedRate", Quantitative).
      mark(Bar).
      show

    // Embarked
    Vegas("A simple bar chart showing Embarked count.").
      withDataFrame(SparkUtils.sql("select Embarked, count(1) as CNT from inputTable group by Embarked")).
      encodeX("Embarked", Nominal).
      encodeY("CNT", Quantitative).
      mark(Bar).
      show

    Vegas("A simple bar chart showing relation bettween Embarked and SurvivedRate.").
      withDataFrame(SparkUtils.sql("select Embarked, avg(Survived) as SurvivedRate from inputTable group by Embarked")).
      encodeX("Embarked", Nominal).
      encodeY("SurvivedRate", Quantitative).
      mark(Bar).
      show

    // TODO: Name -> Title

    SparkUtils.sql("select extractTitle(Name) as title, count(1) from inputTable group by extractTitle(Name)").show


  }
}
