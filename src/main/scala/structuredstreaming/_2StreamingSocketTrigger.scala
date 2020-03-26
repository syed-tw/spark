package structuredstreaming

import global.GlobalClass.spark
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object _2StreamingSocketTrigger {

  def main(args: Array[String]): Unit = {

    //create stream from socket. This acts like  source, the socket in this case
    val socketStreamDf = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999).load()

    //This is writing data to the sink, the console in this case
    val consoleDataFrameWriter = socketStreamDf
      .writeStream.format("console")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime("30 seconds"))

    //This is the query
    val query = consoleDataFrameWriter.start()

    //This starts the query thread to pick up data in the background
    query.awaitTermination()

  }

}
