package main.scala

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{when, broadcast, udf}
import org.apache.log4j.{Level, Logger}


object Main {

  val session = SparkSession
    .builder
    .master("local[2]")
    .appName("CarsPipeline")
    .config("spark.dynamicAllocation.enabled", "true")
    .getOrCreate()

  import session.implicits._


  def main(args: Array[String]) {

    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val rawData = session.read.json("./cars.json.gz")
      // We drop right now the data that we won't be using along the processing and that are not interesting for the users
      // That makes the entire pipeline lighter
      .drop("titleChunk").drop("urlAnonymized").drop("contentChunk")
      // I assumed partitioning by date would result in a fairly even distribution of the data
      .repartition($"date")
      .cache()
    rawData.show(20)
    rawData.printSchema()

    /*** Q1: Deduplicating ***/
    lazy val dedupData = deduplicate(rawData, "uniqueId").cache()
    print("\n Count of uniqueIds : " + dedupData.count() + "\n")


    /*** Q2: Getting insights ***/
    // The DF Baseline contains the average value of mileage, price and year for each (make, model) pair
    // First way of creating the baseline DF. More performant but does not exclude outliers (any 0 value for one of our criteria).
    /***
    val baseline = dedupData.groupBy("make", "model").agg(Map(
      "year" -> "avg",
      "price" -> "avg",
      "mileage" -> "avg"
    ))
    baseline.show(20)
    ***/

    // Second way of creating the baseline DF. Less performant but excludes the outliers thus will give a more accurate final result.
    val yearBaseline = groupAndAverage(dedupData, $"make", $"model", "year")
    val mileageBaseline = groupAndAverage(dedupData, $"make", $"model", "mileage")
    val priceBaseline = groupAndAverage(dedupData, $"make", $"model", "price")
    // After those rows, dedupData is evaluated so we don't need rawData anymore, we can release it from the memory
    rawData.unpersist()
    lazy val betterBaseline = generateBaseline(yearBaseline, mileageBaseline, priceBaseline)
    betterBaseline.show(20)

    // The DF scoring attributes a score to each unique_id by comparing them to our baseline means
    val scoring = attributeScores(dedupData, betterBaseline).cache()
    scoring.show(20)
    dedupData.unpersist()

    // Last step: Gather our scores in a "deal" columns specifying how good (or bad) the offer is for each classified ad
    val insights = identifyDeal(scoring)
    insights.show(20)

    insights.sample(false, 0.01).write.json("./insights_result")

    scoring.unpersist()
  }


  /**
    * Deduplicate a DF on a column
    */
  def deduplicate(rawData: DataFrame, col: String): DataFrame = {
    rawData.dropDuplicates(col)
  }

  /**
    * Group our dedupData DF on (make. model) and avg the chosen column.
    * Take also care of removing the outliers from the chosen column before computing the average.
    */
  def groupAndAverage(dedupData: DataFrame, make: Column, model: Column, avg_col: String): DataFrame = {
    dedupData.where(avg_col + " != 0").groupBy(make, model).avg(avg_col)
  }

  /**
    * Join our different field baseline DFs to generate the final Baseline DF
    */
  def generateBaseline(yearBaseline: DataFrame, mileageBaseline: DataFrame, priceBaseline: DataFrame): DataFrame = {

   yearBaseline.as("y")
     .join(mileageBaseline.as("m"),
       $"y.make" === $"m.make" && $"y.model" === $"m.model",
       "inner").drop($"m.make").drop($"m.model")
     .join(priceBaseline.as("p"),
       $"y.make" === $"p.make" && $"y.model" === $"p.model",
       "inner").drop($"p.make").drop($"p.model")
  }

  /**
    * Attribute a score to our dedupData DF according to each criteria (price, year, mileage)
    */
  def attributeScores(dedupData: DataFrame, betterBaseline: DataFrame): DataFrame = {

    dedupData.as("d")
      .join(broadcast(betterBaseline).as("b"),
      $"d.make" === $"b.make" && $"d.model" === $"b.model",
      "left_outer").drop($"b.make").drop($"b.model")
      .withColumn("score_year", when($"d.year" - $"b.avg(year)" > 3, 1) otherwise 0)
      .withColumn("score_price", when($"b.avg(price)" - $"d.price" > 4000.0, 1) otherwise 0)
      .withColumn("score_mileage", when($"b.avg(mileage)" - $"d.mileage" > 50000, 1) otherwise 0)
      .drop($"avg(year)").drop($"avg(mileage)").drop($"avg(price)")
  }

  /**
    * Compute the total score of each classified ad and assess the quality of the deal
    */
  def identifyDeal(scoring: DataFrame): DataFrame = {

    def assessDeal(s: Int): String = {
      s match {
        case 0 => "Bad"
        case 1 => "Average"
        case 2 => "Interesting"
        case 3 => "Good"
        case _ => "Unknown"
      }
    }
    val assessDeal_udf = udf(assessDeal _)

    scoring.select(
      // Ugly but I did not find a way to merge those 2 queries without hardcoding all the column names and I was running out of time.
      $"uniqueId", $"carType", $"city", $"color", $"country", $"date", $"doors", $"fuel", $"make", $"mileage", $"model", $"price",
      $"region", $"transmission", $"year",
      $"score_year" + $"score_price" + $"score_mileage").withColumnRenamed("((score_year + score_price) + score_mileage)", "score")
      .withColumn("deal", assessDeal_udf($"score"))
      .drop($"score")
  }

}