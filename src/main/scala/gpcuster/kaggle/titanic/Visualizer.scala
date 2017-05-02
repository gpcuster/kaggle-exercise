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

    Vegas("A simple bar chart showing relation bettween Pclass and SurvivedRate.").
      withDataFrame(SparkUtils.sql("select Pclass, avg(Survived) as SurvivedRate from inputTable group by Pclass")).
      encodeX("Pclass", Ordinal).
      encodeY("SurvivedRate", Quantitative).
      mark(Bar).
      show

    SparkUtils.getSpark().udf.register("extractTitle", (name: String) => {
      val startIndex = name.indexOf(", ")
      val endIndex = name.indexOf(". ", startIndex)

      name.substring(startIndex + 2, endIndex)
    })

    SparkUtils.sql("select extractTitle(Name) as title, count(1) from inputTable group by extractTitle(Name)").show


  }
}
