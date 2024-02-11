package sources

import helpers.DatasetIO
import org.apache.spark.sql.{Encoder, Encoders}

case class PassengerDetail(
                            passengerId: Option[Int] = None,
                            firstName: Option[String] = None,
                            lastName: Option[String] = None
                          )

object PassengerDetail extends DatasetIO[PassengerDetail]{
  implicit lazy val encoder: Encoder[PassengerDetail] = Encoders.product[PassengerDetail]
}