package jobs

import org.apache.spark.sql.{Dataset, SparkSession}
import sinks.FlightsPerMonth
import sources.PassengerFlight

object TotalFlightsPerMonth {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: Foo <input path>")
      System.exit(1)
    }

    val inputPath = args(0)

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("TotalFlightsPerMonth")
      .getOrCreate()

    import spark.implicits._

    val passengerFlights: Dataset[PassengerFlight] = PassengerFlight.readFromCsv(paths = Seq(inputPath), options = Map("header" -> "true"))

    val result = passengerFlights
      .map(f => (f.getMonthNumber, f.flightId))
      .distinct()
      .map(f => (f._1, 1))
      .rdd
      .reduceByKey(_ + _)
      .toDF("month", "totalFlights")
      .as[FlightsPerMonth]

    result.show()
    spark.stop()
  }
}

