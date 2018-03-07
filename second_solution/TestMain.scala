package main.scala

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}
import main.scala.Main.{deduplicate, groupAndAverage, generateBaseline, attributeScores, identifyDeal}

class TestMain extends FunSuite with BeforeAndAfterEach with Matchers {

  var sparkSession : SparkSession = _

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("Unit Testing")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  override def afterEach() {
    sparkSession.stop()
  }

  test("deduplicate unit test"){
    // A better way to get these mock data would be to generate them from the code in beforeEach as it would make
    // them self-contained and avoid to repeat it in the beginning of each test.
    val testrawData = sparkSession.read.json("./test-sample.json.gz")
    testrawData.count() shouldBe 20
    deduplicate(testrawData, "uniqueId").count() shouldBe 18
    deduplicate(testrawData, "country").count() shouldBe 1
  }

  test("groupAndAverage unit test"){
    val dedupData = deduplicate(sparkSession.read.json("./test-sample.json.gz"), "uniqueId")

    groupAndAverage(dedupData, dedupData("make"), dedupData("model"), "mileage")
          .select("avg(mileage)").collect().head(0) shouldBe 121672.5
    groupAndAverage(dedupData, dedupData("make"), dedupData("model"), "price")
          .select("avg(price)").collect().head(0) shouldBe 7200.0
  }

  test("generateBaseline unit test"){
    val dedupData = deduplicate(sparkSession.read.json("./test-sample.json.gz"), "uniqueId")
    val yearBaseline = groupAndAverage(dedupData, dedupData("make"), dedupData("model"), "year")
    val mileageBaseline = groupAndAverage(dedupData, dedupData("make"), dedupData("model"), "mileage")
    val priceBaseline = groupAndAverage(dedupData, dedupData("make"), dedupData("model"), "price")

    generateBaseline(yearBaseline, mileageBaseline, priceBaseline).columns(2) shouldBe "avg(year)"
    generateBaseline(yearBaseline, mileageBaseline, priceBaseline).columns(3) shouldBe "avg(mileage)"
  }

  /**
    * I won't have any further spare time to implement those last two but the logic would be the same as I did for the groupAverage test:
    * Compute the DF with the functions. Check if the scores and deal values for a few row match what I am expecting.
  */
  test("attributeScores unit test") {}
  test("identifyDeal unit test") {}

}
