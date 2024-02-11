package jobs

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import sinks.PassengerLongestRunOutsideUK
import sources.PassengerFlight

import java.sql.Date

class LongestRunOutsideUKSpec extends AnyFlatSpec {
  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  "UK -> FR -> US -> CN -> UK -> DE -> UK" should "return 3" in {
    val flights: Seq[PassengerFlight] = Seq(
      PassengerFlight(passengerId = Some(1), from = Some("UK"), to = Some("FR"), date = Some(Date.valueOf("2000-01-01"))),
      PassengerFlight(passengerId = Some(1), from = Some("FR"), to = Some("US"), date = Some(Date.valueOf("2000-01-02"))),
      PassengerFlight(passengerId = Some(1), from = Some("US"), to = Some("CN"), date = Some(Date.valueOf("2000-01-03"))),
      PassengerFlight(passengerId = Some(1), from = Some("CN"), to = Some("UK"), date = Some(Date.valueOf("2000-01-04"))),
      PassengerFlight(passengerId = Some(1), from = Some("UK"), to = Some("DE"), date = Some(Date.valueOf("2000-01-05"))),
      PassengerFlight(passengerId = Some(1), from = Some("DE"), to = Some("UK"), date = Some(Date.valueOf("2000-01-06"))),
    )
    val result: Long = LongestRunOutsideUK.longestRunOutsideUKSize(flights)
    assert(result == 3)
  }

  "one flight starting from UK" should "be handled properly" in {
    val flights = Seq(
      PassengerFlight(passengerId = Some(1), from = Some("UK"), to = Some("FR"), date = Some(Date.valueOf("2000-01-01"))),
    )
    val result: Long = LongestRunOutsideUK.longestRunOutsideUKSize(flights)
    assert(result == 1)
  }

  "one ending in UK" should "be handled properly" in {
    val flights = Seq(
      PassengerFlight(passengerId = Some(1), from = Some("FR"), to = Some("UK"), date = Some(Date.valueOf("2000-01-01"))),
    )
    val result: Long = LongestRunOutsideUK.longestRunOutsideUKSize(flights)
    assert(result == 1)
  }

  "national flights" should "contribute with 0" in {
    fail("National flights not implemented")
  }

  it should "aggregate by passenger" in {
    val flights: Dataset[PassengerFlight] = Seq(
      PassengerFlight(passengerId = Some(1), from = Some("UK"), to = Some("FR"), date = Some(Date.valueOf("2000-01-01"))),
      PassengerFlight(passengerId = Some(1), from = Some("FR"), to = Some("PT"), date = Some(Date.valueOf("2000-01-02"))),
      PassengerFlight(passengerId = Some(1), from = Some("PT"), to = Some("UK"), date = Some(Date.valueOf("2000-02-01"))),
      PassengerFlight(passengerId = Some(2), from = Some("PT"), to = Some("UK"), date = Some(Date.valueOf("2000-02-01"))),
    )
      .toDS
    val result: Array[PassengerLongestRunOutsideUK] = LongestRunOutsideUK.process(flights).collect()
    assert(result.length == 2)
  }
}