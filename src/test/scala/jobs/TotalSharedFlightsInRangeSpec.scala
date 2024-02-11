package jobs

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import sinks.PassengerSharedFlightsInRange
import sources.PassengerFlight

import java.sql.Date

class TotalSharedFlightsInRangeSpec extends AnyFlatSpec {
  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  it should "filter dates outside the range" in {
    val flights: Dataset[PassengerFlight] = Seq(
      PassengerFlight(passengerId = Some(1), flightId = Some(1), date = Some(Date.valueOf("2000-01-01"))),
      PassengerFlight(passengerId = Some(1), flightId = Some(1), date = Some(Date.valueOf("2010-01-01"))),
      PassengerFlight(passengerId = Some(2), flightId = Some(1), date = Some(Date.valueOf("2010-01-01"))),
      PassengerFlight(passengerId = Some(3), flightId = Some(1), date = Some(Date.valueOf("2010-01-01"))),
      PassengerFlight(passengerId = Some(1), flightId = Some(1), date = Some(Date.valueOf("2020-01-01"))),

    )
      .toDS

    val fromDate: Date = Date.valueOf("2009-12-31")
    val toDate: Date = Date.valueOf("2010-01-02")

    val result: Array[PassengerSharedFlightsInRange] = TotalSharedFlightsInRange.process(flights, fromDate, toDate, minFlights=1).collect()
    assert(result.length == 3)
    assert(result.find(f => f.firstPassengerId.contains(1) && f.secondPassengerId.contains(2)).exists(f => f.totalFlightsTogether == 1))
    assert(result.find(f => f.firstPassengerId.contains(1) && f.secondPassengerId.contains(3)).exists(f => f.totalFlightsTogether == 1))
    assert(result.find(f => f.firstPassengerId.contains(2) && f.secondPassengerId.contains(3)).exists(f => f.totalFlightsTogether == 1))
  }
}