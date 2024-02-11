package sinks

import helpers.DatasetIO
import org.apache.spark.sql.{Encoder, Encoders}

case class PassengerTotalFlights(
                                  passengerId: Option[Int] = None,
                                  totalFlights: Long
                                )

object PassengerTotalFlights extends DatasetIO[PassengerTotalFlights] {
  implicit lazy val encoder: Encoder[PassengerTotalFlights] = Encoders.product[PassengerTotalFlights]
}