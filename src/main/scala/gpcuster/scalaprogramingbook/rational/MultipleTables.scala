package gpcuster.scalaprogramingbook.rational

object MultipleTables {
  def main(args: Array[String]): Unit = {
    val max = 32
    for (indexY <- 1 to max) {
      for (indexX <- 1 to max) {
        val currentNumber = indexY * indexX
        val maxNumber = max * indexX

        val delta = maxNumber.toString.length - currentNumber.toString.length

        if (indexX != 1) {
          print(" ")
        }

        val padding = if (indexY == max) 0 else delta
        print(" " * padding)

        print(currentNumber)
      }
      println()
    }
  }
}
