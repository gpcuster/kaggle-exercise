package gpcuster

import gpcuster.kaggle.util.SparkUtils
import vegas._
import vegas.sparkExt._

object VegasExample {
  def main(args: Array[String]): Unit = {
    Vegas("A simple bar chart with embedded data.").
      withData(Seq(
        Map("a" -> "A", "b" -> 28), Map("a" -> "B", "b" -> 55), Map("a" -> "C", "b" -> 43),
        Map("a" -> "D", "b" -> 91), Map("a" -> "E", "b" -> 81), Map("a" -> "F", "b" -> 53),
        Map("a" -> "G", "b" -> 19), Map("a" -> "H", "b" -> 87), Map("a" -> "I", "b" -> 52)
      )).
      encodeX("a", Ordinal).
      encodeY("b", Quantitative).
      mark(Bar).
      show

    val df = SparkUtils.getSpark().createDataFrame(Seq(
      ("a", 1.0),
      ("b", 120.0),
      ("c", 23.1),
      ("d", 56.1)
    )).toDF("label", "count")

    Vegas("A simple bar chart with spark dataframe data.").
      withDataFrame(df).
      encodeX("label", Ordinal).
      encodeY("count", Quantitative).
      mark(Bar).
      show
  }
}
