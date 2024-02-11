package jobs

import org.apache.spark.sql.{Column, Dataset, SaveMode, SparkSession}
import sinks.PassengerSharedFlightsInRange
import sources.PassengerFlight

import java.sql.Date
import scala.util.Try

object TotalSharedFlightsInRange {

  implicit val optionDateOrdering: Ordering[Option[Date]] =
    (x: Option[Date], y: Option[Date]) => (x, y) match {
      case (None, None) => 0 // equal
      case (None, _) => -1 // x before y
      case (_, None) => 1 // x after y
      case (Some(xd), Some(yd)) => xd.compareTo(yd)
    }

  def main(args: Array[String]): Unit = {
    /*
    Parse
    Read
    Process
    Write
     */
    if (args.length < 4) {
      println("Usage: <flight-data-filepath> <passenger-data-filepath> <from-date> <to-date> <min-num-flights-together")
      println("Example: data/foo.csv data/bar.csv 2022-01-01 2022-01-01 10")
      System.exit(1)
    }

    val inputPath = args(0)
    val fromDateArg = Try(Date.valueOf(args(2))).toOption
    val toDateArg = Try(Date.valueOf(args(3))).toOption
    val minFlightsArg = Try(args(4).toInt).toOption

    val (fromDate: Date, toDate: Date, minFlights: Int) = (fromDateArg, toDateArg, minFlightsArg) match {
      case (Some(a), Some(b), Some(c)) => (a, b, c)
      case _ =>
        System.err.println("Invalid argument format. See usage.")
        System.exit(1)
    }

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("TotalSharedFlightsInRange")
      .getOrCreate()


    val passengerFlights = PassengerFlight.readFromCsv(paths = Seq(inputPath), options = Map("header" -> "true"))

    val result = process(passengerFlights, fromDate, toDate, minFlights)

    result
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/app/output/totalSharedFlightsInRange")

    spark.stop()
  }

  def process(
               passengerFlights: Dataset[PassengerFlight],
               fromDate: Date,
               toDate: Date,
               minFlights: Int = 3
             )(implicit spark: SparkSession): Dataset[PassengerSharedFlightsInRange] = {
    import spark.implicits._

    val filteredPassengerFlights = passengerFlights
      .filter(f => f.date.exists(_.after(fromDate)))
      .filter(f => f.date.exists(_.before(toDate)))

    val sameFlightCond: Column = $"a.flightId" === $"b.flightId"
    val diffPassenger: Column = $"a.passengerId" < $"b.passengerId"

    filteredPassengerFlights.as("a")
      .joinWith(passengerFlights.as("b"), condition = sameFlightCond && diffPassenger)
      .groupByKey { case (a, b) => (a.passengerId, b.passengerId) }
      .flatMapGroups {
        case (key, iterator) =>
          val dates = iterator
            .map { case (a, _) => a.date }
            .toSeq
          val minDate = dates.min(optionDateOrdering)
          val maxDate = dates.max(optionDateOrdering)
          Iterator(
            PassengerSharedFlightsInRange(
              firstPassengerId = key._1,
              secondPassengerId = key._2,
              totalFlightsTogether = dates.size,
              fromDate = minDate,
              toDate = maxDate
            )
          )
      }
      .filter(f => f.totalFlightsTogether >= minFlights)
  }
}
