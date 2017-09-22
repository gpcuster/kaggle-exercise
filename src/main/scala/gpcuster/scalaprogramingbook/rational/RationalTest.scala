package gpcuster.scalaprogramingbook.rational

object RationalTest {
  def main(args: Array[String]): Unit = {
    println(Rational(1,3))
    println(Rational(2,6))
    println(Rational(3))
    println(-Rational(3))

    //assert(Rational(3) == 3)
    assert(Rational(3,2) == Rational(3,2))
    assert(Rational(3) != null)
    assert(null != Rational(3))

    println(Rational(1,2) + (Rational(2,3)))
    println(Rational(1,2) - (Rational(2,3)))
    println(Rational(1,2) * (Rational(2,5)))
    println(Rational(1,2) / (Rational(3,5)))

    println(3 + (Rational(2,3)))
  }
}
