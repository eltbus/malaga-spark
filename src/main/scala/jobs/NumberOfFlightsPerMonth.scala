package jobs

import org.apache.spark.sql.{Dataset, SparkSession}
import sources.PassengerFlight

object NumberOfFlightsPerMonth {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: Foo <input path>")
      System.exit(1)
    }

    val inputPath = args(0)

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Foo")
      .getOrCreate()

    import spark.implicits._

    val flights = PassengerFlight.readFromCsv(paths = Seq(inputPath), options = Map("header" -> "true"))

    val result = flights
      .groupByKey(_.getMonthNumber)
      .count()
      .orderBy("value")

    result.show()
    spark.stop()
  }
}

