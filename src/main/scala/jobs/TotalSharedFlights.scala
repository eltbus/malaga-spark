package jobs

import org.apache.spark.sql.{Column, Dataset, SaveMode, SparkSession}
import sinks.PassengerSharedFlights
import sources.PassengerFlight

object TotalSharedFlights {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: <flight-data-filepath>")
      System.exit(1)
    }

    val inputPath = args(0)

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("TotalSharedFlights")
      .getOrCreate()

    import spark.implicits._

    val passengerFlights: Dataset[PassengerFlight] = PassengerFlight.readFromCsv(paths = Seq(inputPath), options = Map("header" -> "true"))

    val sameFlightCond: Column = $"a.flightId" === $"b.flightId"
    val diffPassenger: Column = $"a.passengerId" < $"b.passengerId"

    val result = passengerFlights.as("a")
      .joinWith(
        passengerFlights.as("b"),
        condition = sameFlightCond && diffPassenger,
      )
      .map(f => ((f._1.passengerId, f._2.passengerId), 1))
      .rdd
      .reduceByKey(_ + _)
      .map { case (keyPair, count) => (keyPair._1, keyPair._2, count) }
      .toDF("firstPassengerId", "secondPassengerId", "totalFlightsTogether")
      .as[PassengerSharedFlights]
      .filter(f => f.totalFlightsTogether > 3)

    result
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/app/output/totalSharedFlights")

    spark.stop()
  }
}
