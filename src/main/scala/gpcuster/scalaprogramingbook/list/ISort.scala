package gpcuster.scalaprogramingbook.list

object ISort {
  def main(args: Array[String]): Unit = {
    def insert(x: Int, xs: List[Int]): List[Int] = {
      if (xs.isEmpty || x <= xs.head) x :: xs
      else xs.head :: insert(x, xs.tail)
    }

    val testList = 3::12::1::43::2::Nil
    println(insert(10, testList))

    def isort(xs: List[Int]): List[Int] = {
      if (xs.isEmpty) Nil
      else insert(xs.head, isort(xs.tail))
    }

    println(isort(testList))
  }
}
