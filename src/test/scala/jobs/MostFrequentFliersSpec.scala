package jobs

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import sinks.DetailedPassengerTotalFlights
import sources.{PassengerDetail, PassengerFlight}

import java.sql.Date

class MostFrequentFliersSpec extends AnyFlatSpec {
  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  it should "limit the amount of passengers" in {
    val flights: Dataset[PassengerFlight] = Seq(
      PassengerFlight(passengerId = Some(1), from = Some("UK"), to = Some("FR"), date = Some(Date.valueOf("2000-01-01"))),
      PassengerFlight(passengerId = Some(1), from = Some("FR"), to = Some("PT"), date = Some(Date.valueOf("2000-01-02"))),
      PassengerFlight(passengerId = Some(1), from = Some("PT"), to = Some("UK"), date = Some(Date.valueOf("2000-02-01"))),
      PassengerFlight(passengerId = Some(2), from = Some("PT"), to = Some("UK"), date = Some(Date.valueOf("2000-02-01"))),
    )
      .toDS

    val passengers: Dataset[PassengerDetail] = Seq(
      PassengerDetail(passengerId = Some(1), firstName = Some("Foo"), lastName = Some("Bar")),
      PassengerDetail(passengerId = Some(2), firstName = Some("Spam"), lastName = Some("Eggs")),
    )
      .toDS
    val result: Array[DetailedPassengerTotalFlights] = MostFrequentFliers.process(flights, passengers, 1).collect()

    assert(result.length == 1)
    assert(result.exists(p => p.firstName.contains("Foo")))
    assert(result.exists(p => p.totalFlights == 3))
  }
}