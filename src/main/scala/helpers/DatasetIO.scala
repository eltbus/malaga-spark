package helpers

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

trait DatasetIO[T <: Product] {
  implicit val encoder: Encoder[T]

  def readFromCsv(paths: Seq[String], options: Map[String, String])(implicit spark: SparkSession): Dataset[T] = {
    spark
      .read
      .format("csv")
      .options(options)
      .schema(encoder.schema)
      .load(paths: _*)
      .as[T]
  }
}