package jobs

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import sinks.FlightsPerMonth
import sources.PassengerFlight

import java.sql.Date

class TotalFlightsPerMonthSpec extends AnyFlatSpec {
  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  it should "remove duplicated flights" in {
    val flights: Dataset[PassengerFlight] = Seq(
      PassengerFlight(passengerId = Some(1), flightId = Some(1), date = Some(Date.valueOf("2000-01-01"))),
      PassengerFlight(passengerId = Some(2), flightId = Some(1), date = Some(Date.valueOf("2000-01-01"))),
      PassengerFlight(passengerId = Some(3), flightId = Some(1), date = Some(Date.valueOf("2000-01-01"))),
      PassengerFlight(passengerId = Some(1), flightId = Some(2), date = Some(Date.valueOf("2000-02-01"))),
      PassengerFlight(passengerId = Some(2), flightId = Some(3), date = Some(Date.valueOf("2000-02-02"))),
    )
      .toDS
    val result: Array[FlightsPerMonth] = TotalFlightsPerMonth.process(flights).collect()
    assert(result.length == 2)
    assert(result.find(f => f.month == 1).exists(f => f.totalFlights == 1))
    assert(result.find(f => f.month == 2).exists(f => f.totalFlights == 2))
  }
}