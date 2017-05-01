package gpcuster

import vegas._
import vegas.data.External._

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
  }
}
