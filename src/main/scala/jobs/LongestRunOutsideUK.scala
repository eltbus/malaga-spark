package jobs

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import sinks.PassengerLongestRunOutsideUK
import sources.PassengerFlight

import java.sql.Date

object LongestRunOutsideUK {

  implicit val optionDateOrdering: Ordering[Option[Date]] =
    (x: Option[Date], y: Option[Date]) => (x, y) match {
      case (None, None) => 0 // equal
      case (None, _) => -1 // x before y
      case (_, None) => 1 // x after y
      case (Some(xd), Some(yd)) => xd.compareTo(yd)
    }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: <flight-data-filepath>")
      System.exit(1)
    }

    val inputPath = args(0)

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("LongestRunOutsideUK")
      .getOrCreate()

    import spark.implicits._

    val passengerFlights: Dataset[PassengerFlight] = PassengerFlight.readFromCsv(paths = Seq(inputPath), options = Map("header" -> "true"))

    val result = passengerFlights
      .groupByKey(f => f.passengerId)
      .flatMapGroups( // TODO: m
        (passengerId, flightsIterator) => {
          val sortedFlights = flightsIterator.toSeq.sortBy(_.date) // TODO: nlog(n)
          val longestSequence = longestRunOutsideUK(sortedFlights) // TODO: n
          Iterator(
            PassengerLongestRunOutsideUK(passengerId, longestSequence)
          )
        }
      )

    result
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/app/output/longestRunOutsideUK")

    spark.stop()
  }

  private def longestRunOutsideUK(flights: Seq[PassengerFlight]): Long = { // TODO: O(n) = n

    var maxCount: Long = 1 // NOTE: assuming only international flights
    var currentCount: Long = 0
    flights.foreach { f =>
      if (f.to.map(_.toLowerCase).contains("uk")) {
        currentCount = 0
      } else {
        currentCount += 1
        maxCount = Math.max(maxCount, currentCount)
      }
    }
    maxCount
  }
}
