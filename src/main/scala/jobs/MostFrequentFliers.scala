package jobs

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import sinks.{DetailedPassengerTotalFlights, PassengerTotalFlights}
import sources.{PassengerDetail, PassengerFlight}

object MostFrequentFliers {
  def main(args: Array[String]): Unit = {
    /*
    Parse
    Read
    Process
    Write
     */
    if (args.length < 2) {
      println("Usage: <flight-data-filepath> <passenger-data-filepath>")
      System.exit(1)
    }

    val inputPath1 = args(0)
    val inputPath2 = args(1)

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("MostFrequentFliers")
      .getOrCreate()

    val passengerFlights = PassengerFlight.readFromCsv(paths = Seq(inputPath1), options = Map("header" -> "true"))
    val passengerDetails = PassengerDetail.readFromCsv(paths = Seq(inputPath2), options = Map("header" -> "true"))

    val result = process(passengerFlights, passengerDetails)

    result
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/app/output/mostFrequentFliers")

    spark.stop()
  }

  def process(
               passengerFlights: Dataset[PassengerFlight],
               passengerDetails: Dataset[PassengerDetail],
               limit: Int = 100
             )(implicit spark: SparkSession): Dataset[DetailedPassengerTotalFlights] = {
    import spark.implicits._

    val top100 = passengerFlights
      .map(f => (f.passengerId, 1))
      .rdd
      .reduceByKey(_ + _)
      .toDF("passengerId", "totalFlights")
      .as[PassengerTotalFlights]
      .orderBy(col("totalFlights").desc)
      .limit(limit)

    top100
      .joinWith(passengerDetails, condition = top100("passengerId") === passengerDetails("passengerId"))
      .map(
        f => DetailedPassengerTotalFlights(
          passengerId = f._1.passengerId,
          totalFlights = f._1.totalFlights,
          firstName = f._2.firstName,
          lastName = f._2.lastName
        )
      )
  }
}