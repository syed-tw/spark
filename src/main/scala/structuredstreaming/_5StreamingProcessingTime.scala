package structuredstreaming

import global.GlobalClass.spark
import java.sql.Timestamp
import org.apache.spark.sql.functions._

import org.apache.spark.sql.streaming.OutputMode

/**
  * Here we are taking all the data as it has come in the last 1 minute
  * If the data comes after 1 minute, it will go in the next window.
  */
object _5StreamingProcessingTime {

  def main(args: Array[String]): Unit = {


    //Read the data coming from the socket as usual
    val socketStreamDf = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    /***
      * Since we are defining a time window here,
      * we need to have a column in the above dataframe as timestamp
      * Lets take the current timestamp for the same as below.
      * This is the time after the data is received from the socket stream
      * i.e a time stamp is added at the processing system level as soon as
      * the data arrives in the processing engine
      */

    val currentTimeDf = socketStreamDf.withColumn("processingTime",current_timestamp())

    //implicits for encoders
    import spark.implicits._

    /***
      * The below code will convert the stream coming from sockets
      * into a dataset, An API that is only present in scala version
      * of Spark as of now. It will then chop the words on the basis of space
      * It will then map the word with respective received timestamps.
      */
    val socketDs = currentTimeDf.as[(String, Timestamp)]
    val wordsDs = socketDs
      .flatMap(line => line._1.split(" ").map(word => (word, line._2)))
      .toDF("word", "processingTime")

    //wordsDs.printSchema()

    /***
      * Once we have words, we define a tumbling window (fixed time)
      * which aggregates data for last 30 seconds.
      */

    val windowedCountDF = wordsDs.groupBy(
        window($"processingTime", "30 seconds"), $"word")
      .count().alias("count")
      .orderBy("window")

   // windowedCountDF.printSchema()

    // Create the query
    val query =
      windowedCountDF.writeStream
        .format("console")
        .option("truncate","false")
        .outputMode(OutputMode.Complete())

    //run it !
    query.start().awaitTermination()
  }
}

