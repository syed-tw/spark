package structuredstreaming


import java.sql.Timestamp

import global.GlobalClass.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode


object _6StreamingIngestionTime {

  def main(args: Array[String]): Unit = {


    //create stream from socket

    val socketStreamDf = spark.readStream
      .format("socket").option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", true).load()

    import spark.implicits._

    //Now the timestamp data is also coming with the stream
    val socketDs = socketStreamDf.as[(String, Timestamp)]
    //val socketDs = socketStreamDf.as[String]



    val wordsDs = socketDs
      .flatMap(line => line._1.split(" ").map(word => {
        //Thread.sleep(15000)
        (word, line._2)
      }))
      .toDF("word", "ingestionTime")


   val windowedCount = wordsDs
      .groupBy(
        window($"ingestionTime", "15 seconds"), $"word")
      .count()
      .orderBy("window")

   //Aggregation code here below
   //val countDs = wordsDs.groupBy("word", "timestamp").count().orderBy("timestamp")



    val query =
      windowedCount.writeStream
        .format("console").option("truncate","false")
        .outputMode(OutputMode.Complete()).start()



    query.awaitTermination()
  }
}

