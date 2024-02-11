package sources

import java.sql.Date
import org.apache.spark.sql.{Encoder, Encoders}
import helpers.DatasetIO

case class PassengerFlight(
                            passengerId: Option[Int] = None,
                            flightId: Option[Int] = None,
                            from: Option[String] = None,
                            to: Option[String] = None,
                            date: Option[Date] = None
                          ) {
  def getMonthNumber: Option[Int] = {
    date.map(_.toLocalDate.getMonthValue)
  }
}

object PassengerFlight extends DatasetIO[PassengerFlight] {
  implicit lazy val encoder: Encoder[PassengerFlight] = Encoders.product[PassengerFlight]
}