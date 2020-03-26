package structuredstreaming


import java.sql.Timestamp

import global.GlobalClass.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

/***
  * Here we are taking data generated at a particular time stamp
  * For this reason, we are embedding the time stamp in the dataset as it
  * arrives (see the code from line 36 to 38)
  * So here, we will be sending the data with timestamp
  * We will see that the data for a particular timestamp will land up in
  * the respective window.
  */

object _7StreamingEventTime {

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

    // Now that th
    val stockDs = socketStreamDs.map(value => {
      val columns = value.split(",")
      Stock(new Timestamp(columns(0).toLong), columns(1), columns(2).toDouble)
    })

    val windowedCount = stockDs
      .groupBy(
        window($"time", "10 seconds")
      )
      .sum("value")


    val query =
      windowedCount.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode(OutputMode.Complete())

    query.start().awaitTermination()
  }
}

