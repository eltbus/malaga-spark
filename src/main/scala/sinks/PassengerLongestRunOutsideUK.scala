package sinks

import helpers.DatasetIO
import org.apache.spark.sql.{Encoder, Encoders}

case class PassengerLongestRunOutsideUK(
                                         passengerId: Option[Int],
                                         longestRun: Long,
                                       )

object PassengerLongestRunOutsideUK extends DatasetIO[PassengerLongestRunOutsideUK] {
  implicit lazy val encoder: Encoder[PassengerLongestRunOutsideUK] = Encoders.product[PassengerLongestRunOutsideUK]
}