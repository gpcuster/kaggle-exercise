package gpcuster.kaggle.util

object GlobalUDFs {
  def registerUDFs = {
    Utils.getSpark().udf.register("convertNumberToIntOrZero", (str: String) => Option(str) match {
      case Some(number) => try {
        number.toInt
      } catch {
        case _ => 0
      }
      case _ => 0
    })
  }
}
