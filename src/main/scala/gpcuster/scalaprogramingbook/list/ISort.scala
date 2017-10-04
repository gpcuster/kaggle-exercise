package gpcuster.scalaprogramingbook.list

object ISort {
  def main(args: Array[String]): Unit = {
    def insert(x: Int, xs: List[Int]): List[Int] = {
      xs match {
        case List() => List(x)
        case y :: ys => if (x <= y) x::xs
                        else y::insert(x, ys)
      }
    }

    def isort(xs: List[Int]): List[Int] = {
      xs match {
        case List() => List()
        case x :: xs1 => insert(x, isort(xs1))
      }
    }

    val testList = 3::12::1::43::2::Nil
    println(isort(testList))
  }
}
