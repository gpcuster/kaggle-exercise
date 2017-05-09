package gpcuster.kaggle.titanic

import gpcuster.kaggle.util.SparkUtils
import org.apache.spark.sql.DataFrame
import vegas._
import vegas.sparkExt._
import vegas.spec.Spec.AggregateOpEnums.{Average, Count}

object Visualizer {
  def visualize(inputDF: DataFrame) = {

    inputDF.createOrReplaceTempView("inputTable")

    inputDF.show(10, false)

    inputDF.describe().show()

    // Pclass
    Vegas("A simple bar chart showing relation bettween Pclass and SurvivedRate.").
      withDataFrame(SparkUtils.sql("select Pclass, avg(Survived) as SurvivedRate from inputTable group by Pclass")).
      encodeX("Pclass", Nominal).
      encodeY("SurvivedRate", Quantitative).
      mark(Bar).
      show

    // Embarked
    Vegas("A simple bar chart showing Embarked count.").
      withDataFrame(SparkUtils.sql("select Embarked, count(1) as CNT from inputTable group by Embarked")).
      encodeX("Embarked", Nominal).
      encodeY("CNT", Quantitative).
      mark(Bar).
      show

    Vegas("A simple bar chart showing relation bettween Embarked and SurvivedRate.").
      withDataFrame(SparkUtils.sql("select Embarked, avg(Survived) as SurvivedRate from inputTable group by Embarked")).
      encodeX("Embarked", Nominal).
      encodeY("SurvivedRate", Quantitative).
      mark(Bar).
      show

    // Fare
    Vegas("A simple bar chart showing relation bettween Fare and SurvivedRate.").
      withDataFrame(SparkUtils.sql("select convertDouble(Fare) as Fare_INT, avg(Survived) as SurvivedRate from inputTable group by convertDouble(Fare)")).
      encodeX("Fare_INT", Quantitative).
      encodeY("SurvivedRate", Quantitative).
      mark(Bar).
      show

    Vegas("A simple bar chart with bin X showing relation bettween Fare and SurvivedRate.").
      withDataFrame(SparkUtils.sql("select convertDouble(Fare) as Fare_INT, avg(Survived) as SurvivedRate from inputTable group by convertDouble(Fare)")).
      encodeX("Fare_INT", Quantitative, bin=Bin(maxbins=10.0)).
      encodeY("SurvivedRate", Quantitative, aggregate = Average).
      mark(Point).
      show

    // Age
    Vegas("A simple bar chart showing relation bettween Age and Survived.").
      withDataFrame(SparkUtils.sql("select convertDouble(Age) as Age_INT, Survived from inputTable")).
      encodeX("Age_INT", Quantitative, bin=Bin(maxbins=50.0)).
      encodeY("Age_INT", Quantitative, aggregate = Count).
      mark(Line).
      encodeColor("Survived", Nominal).
      show

    // Family
    Vegas("A simple bar chart showing relation bettween Family and SurvivedRate.").
      withDataFrame(SparkUtils.sql("select hasFamily(SibSp, Parch) AS hasFamily, avg(Survived) as SurvivedRate from inputTable group by hasFamily(SibSp, Parch)")).
      encodeX("hasFamily", Nominal).
      encodeY("SurvivedRate", Quantitative).
      mark(Bar).
      show

    // Sex
    Vegas("A simple bar chart showing relation bettween Sex and SurvivedRate.").
      withDataFrame(SparkUtils.sql("select Sex, avg(Survived) as SurvivedRate from inputTable group by Sex")).
      encodeX("Sex", Nominal).
      encodeY("SurvivedRate", Quantitative).
      mark(Bar).
      show

    // TODO: Name -> Title

    SparkUtils.sql("select extractTitle(Name) as title, count(1) from inputTable group by extractTitle(Name)").show


  }
}
