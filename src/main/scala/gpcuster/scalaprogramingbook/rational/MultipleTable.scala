package gpcuster.scalaprogramingbook.rational

object MultipleTable {

  def makeRow(startX: Int, startY: Int, indexY: Int) = {
    for (indexX <- startX to startY) yield {
      val currentNumber = indexY * indexX
      val maxNumber = startY * indexX

      val padding = maxNumber.toString.length - currentNumber.toString.length

      " " * padding + currentNumber
    }
  }

  def makeRows(startX: Int, startY: Int) = {
    for (indexY <- startX to startY) yield {

      val row = makeRow(startX, startY, indexY)

      row.mkString(" ")
    }
  }

  def makeTable(startX: Int, startY: Int) = {
    val table = makeRows(startX, startY)

    table.mkString("\n")
  }

  def main(args: Array[String]): Unit = {
    val startX = 4
    val startY = 32

    val ret = makeTable(startX, startY)

    print(ret)
  }

}
