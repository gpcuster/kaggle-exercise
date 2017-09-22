package gpcuster.scalaprogramingbook.rational

class Rational(n:Int,d:Int) {
  require(d != 0)

  private val g = gcd(n.abs, d.abs)
  private val numerator = n / g
  private val denominator = d / g

  def this(numerator:Int) = this(numerator, 1)

  private def gcd(a:Int, b:Int): Int = {
    if (b == 0) {
      a
    } else {
      gcd(b, a % b)
    }
  }

  override def equals(other: scala.Any): Boolean = {
    if (other.isInstanceOf[Rational]) {
      val otherRational = other.asInstanceOf[Rational]
      if (numerator == otherRational.numerator && denominator == otherRational.denominator) {
        return true
      }
    }

    return false
  }

  override def toString: String = {
    if (denominator == 1) {
      numerator.toString
    } else {
      s"$numerator / $denominator"
    }
  }

  def unary_- = {
    new Rational(-numerator, denominator)
  }

  def + (other: Rational) = {
    new Rational(numerator * other.denominator + other.numerator * denominator, denominator * other.denominator)
  }

  def - (other: Rational) = {
    this + new Rational(-other.numerator, other.denominator)
  }

  def * (other: Rational) = {
    new Rational(numerator * other.numerator, denominator * other.denominator)
  }

  def / (other: Rational) = {
    this * new Rational(other.denominator, other.numerator)
  }
}

object Rational {
  def apply(numerator: Int, denominator: Int): Rational = new Rational(numerator, denominator)
  def apply(numerator: Int): Rational = new Rational(numerator)

  implicit def int2Rational(i:Int) = new Rational(i)
}
