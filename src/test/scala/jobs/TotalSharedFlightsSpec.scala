package jobs

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import sinks.PassengerSharedFlights
import sources.PassengerFlight

class TotalSharedFlightsSpec extends AnyFlatSpec {
  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  it should "not have redundant duplicates" in {
    val flights: Dataset[PassengerFlight] = Seq(
      PassengerFlight(passengerId = Some(1), flightId = Some(1)),
      PassengerFlight(passengerId = Some(2), flightId = Some(1)),
      PassengerFlight(passengerId = Some(3), flightId = Some(1)),
    )
      .toDS

    val result: Array[PassengerSharedFlights] = TotalSharedFlights.process(flights, limit=1).collect()
    assert(result.length == 3)
    assert(result.find(f => f.firstPassengerId.contains(1) && f.secondPassengerId.contains(2)).exists(f => f.totalFlightsTogether == 1))
    assert(result.find(f => f.firstPassengerId.contains(1) && f.secondPassengerId.contains(3)).exists(f => f.totalFlightsTogether == 1))
    assert(result.find(f => f.firstPassengerId.contains(2) && f.secondPassengerId.contains(3)).exists(f => f.totalFlightsTogether == 1))
  }
}