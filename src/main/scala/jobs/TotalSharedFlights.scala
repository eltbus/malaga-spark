package jobs

import org.apache.spark.sql.{Column, Dataset, SaveMode, SparkSession}
import sinks.PassengerSharedFlights
import sources.PassengerFlight

object TotalSharedFlights {
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
      .appName("TotalSharedFlights")
      .getOrCreate()

    val passengerFlights = PassengerFlight.readFromCsv(paths = Seq(inputPath), options = Map("header" -> "true"))

    val result = process(passengerFlights)

    result
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/app/output/totalSharedFlights")

    spark.stop()
  }

  def process(passengerFlights: Dataset[PassengerFlight], limit: Int = 3)(implicit spark: SparkSession): Dataset[PassengerSharedFlights] = {
    import spark.implicits._

    val sameFlightCond: Column = $"a.flightId" === $"b.flightId"
    val diffPassenger: Column = $"a.passengerId" < $"b.passengerId"

    passengerFlights.as("a")
      .joinWith(passengerFlights.as("b"), condition = sameFlightCond && diffPassenger)
      .map(f => ((f._1.passengerId, f._2.passengerId), 1))
      .rdd
      .reduceByKey(_ + _)
      .map { case (keyPair, count) => (keyPair._1, keyPair._2, count) }
      .toDF("firstPassengerId", "secondPassengerId", "totalFlightsTogether")
      .as[PassengerSharedFlights]
      .filter(f => f.totalFlightsTogether >= limit)
  }
}
