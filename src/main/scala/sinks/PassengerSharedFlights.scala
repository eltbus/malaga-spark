package sinks

import helpers.DatasetIO
import org.apache.spark.sql.{Encoder, Encoders}

case class PassengerSharedFlights(
                                   firstPassengerId: Option[Int],
                                   secondPassengerId: Option[Int],
                                   totalFlightsTogether: Long,
                                 )

object PassengerSharedFlights extends DatasetIO[PassengerSharedFlights] {
  implicit lazy val encoder: Encoder[PassengerSharedFlights] = Encoders.product[PassengerSharedFlights]
}