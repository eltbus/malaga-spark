package sinks

import helpers.DatasetIO
import org.apache.spark.sql.{Encoder, Encoders}

case class DetailedPassengerTotalFlights(
                                  passengerId: Option[Int] = None,
                                  totalFlights: Long,
                                  firstName: Option[String] = None,
                                  lastName: Option[String] = None,
                                )

object DetailedPassengerTotalFlights extends DatasetIO[DetailedPassengerTotalFlights]{
  implicit lazy val encoder: Encoder[DetailedPassengerTotalFlights] = Encoders.product[DetailedPassengerTotalFlights]
}