package gpcuster.scalaprogramingbook.element

object Element {
  abstract class Element {
    def contents: Array[String]
    def height: Int = contents.length
    def width: Int = if (height == 0) 0 else contents(0).length
  }

  class ArrayElement(conts: Array[String]) extends Element {
    override val contents: Array[String] = conts
  }

  class ArrayElement2(var contents: Array[String]) extends Element {
  }

  class ListElement(s: String) extends ArrayElement(Array(s)) {
    def this(i: Int) = this(i.toString)

    override def height: Int = s.length

    override def width: Int = 1
  }

  def main(args: Array[String]): Unit = {
    val element = new ArrayElement2(Array("hello", "scala"))

    element.contents = Array("hey")

    println(element.height)
  }
}
