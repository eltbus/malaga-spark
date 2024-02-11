package jobs

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import sinks.FlightsPerMonth
import sources.PassengerFlight

object TotalFlightsPerMonth {
  def main(args: Array[String]): Unit = {
    /*
    Parse
    Read
    Process
    Write
     */
    if (args.length < 1) {
      println("Usage: <flight-data-filepath>")
      System.exit(1)
    }

    val inputPath = args(0)

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("TotalFlightsPerMonth")
      .getOrCreate()

    val passengerFlights = PassengerFlight.readFromCsv(paths = Seq(inputPath), options = Map("header" -> "true"))

    val result: Dataset[FlightsPerMonth] = process(passengerFlights)

    result
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/app/output/totalFlightsPerMonth")

    spark.stop()
  }

  def process(passengerFlights: Dataset[PassengerFlight])(implicit spark: SparkSession): Dataset[FlightsPerMonth] = {
    import spark.implicits._

    passengerFlights
      .map(f => (f.getMonthNumber, f.flightId))
      .distinct()
      .map(f => (f._1, 1))
      .rdd
      .reduceByKey(_ + _)
      .toDF("month", "totalFlights")
      .as[FlightsPerMonth]
      .orderBy("month")
  }
}