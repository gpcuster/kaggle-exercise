package gpcuster.scalaprogramingbook.list

object MSort {
  def main(args: Array[String]): Unit = {
    def msort(xs: List[Int]): List[Int] = {

      def merge(xs1: List[Int], xs2: List[Int]): List[Int] = {
        (xs1, xs2) match {
          case (Nil, _) => xs2
          case (_, Nil) => xs1
          case (y1::ys1, y2::ys2) => {
            if (y1 > y2) y2::merge(ys2, xs1)
            else y1::merge(ys1, xs2)
          }
        }
      }

      val n = xs.length / 2
      if (n == 0) xs
      else {
        val (xs1, xs2) = xs splitAt n
        merge(msort(xs1), msort(xs2))
      }

    }

    val testList = 3::12::1::43::2::Nil
    println(msort(testList))

  }
}
