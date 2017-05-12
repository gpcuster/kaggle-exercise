package gpcuster.kaggle.houseprice

import gpcuster.kaggle.util.Utils
import org.apache.spark.sql.DataFrame
import vegas._
import vegas.sparkExt._
import vegas.spec.Spec.AggregateOpEnums.{Average, Count}

object Visualizer {
  def visualize(inputDF: DataFrame) = {

    inputDF.createOrReplaceTempView("inputTable")

    inputDF.printSchema

    inputDF.show(10, false)

    inputDF.describe().show()
  }
}
