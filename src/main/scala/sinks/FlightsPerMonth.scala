package sinks

import helpers.DatasetIO
import org.apache.spark.sql.{Encoder, Encoders}

case class FlightsPerMonth(
                          month: Int,
                          totalFlights: Long
                          )

object FlightsPerMonth extends DatasetIO[FlightsPerMonth]{
  implicit lazy val encoder: Encoder[FlightsPerMonth] = Encoders.product[FlightsPerMonth]
}
