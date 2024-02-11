package sinks

import helpers.DatasetIO
import org.apache.spark.sql.{Encoder, Encoders}

import java.sql.Date

case class PassengerSharedFlightsInRange(
                                          firstPassengerId: Option[Int],
                                          secondPassengerId: Option[Int],
                                          totalFlightsTogether: Long,
                                          fromDate: Option[Date],
                                          toDate: Option[Date],
                                        )

object PassengerSharedFlightsInRange extends DatasetIO[PassengerSharedFlightsInRange] {
  implicit lazy val encoder: Encoder[PassengerSharedFlightsInRange] = Encoders.product[PassengerSharedFlightsInRange]
}