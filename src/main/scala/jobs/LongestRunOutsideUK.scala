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

    val passengerFlights = PassengerFlight.readFromCsv(paths = Seq(inputPath), options = Map("header" -> "true"))

    val result = process(passengerFlights)

    result
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/app/output/longestRunOutsideUKSize")

    spark.stop()
  }

  def process(passengerFlights: Dataset[PassengerFlight])(implicit spark: SparkSession): Dataset[PassengerLongestRunOutsideUK] = {
    import spark.implicits._

    passengerFlights
      .groupByKey(f => f.passengerId)
      .flatMapGroups(
        (passengerId, flightsIterator) => {
          val sortedFlights = flightsIterator.toSeq.sortBy(_.date)
          val longestSequenceSize = longestRunOutsideUKSize(sortedFlights)
          Iterator(PassengerLongestRunOutsideUK(passengerId, longestSequenceSize))
        }
      )
  }

  def longestRunOutsideUKSize(flights: Seq[PassengerFlight]): Long = {
    var maxCount: Long = 1 // NOTE: assume international flights only
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
