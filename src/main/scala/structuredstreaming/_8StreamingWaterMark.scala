package structuredstreaming



import global.GlobalClass.spark
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object _8StreamingWaterMark {

  case class Stock(time: Timestamp, symbol: String, value: Double)

  def main(args: Array[String]): Unit = {


    //create stream from socket

    import spark.implicits._

    val socketStreamDs = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      .as[String]

    // read as stock
    val stockDs = socketStreamDs.map(value => {
      val columns = value.split(",")
      Stock(new Timestamp(columns(0).toLong), columns(1), columns(2).toDouble)
    })

    val windowedCount = stockDs
      .withWatermark("time", "500 milliseconds")
      .groupBy(
        window($"time", "10 seconds")
      )
      .sum("value")

    val query =
      windowedCount.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode(OutputMode.Update())

    query.start().awaitTermination()
  }
}

