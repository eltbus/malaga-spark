package jobs

import org.apache.spark.sql.{SparkSession, DataFrame}

object Foo {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: MySparkApp <input path>")
      System.exit(1)
    }

    val inputPath = args(0)
    val spark = SparkSession.builder()
      .appName("My Simple Spark App")
      .getOrCreate()

    val data = spark.read.option("header", "true").csv(inputPath)
    data.show()
    spark.stop()
  }
}

