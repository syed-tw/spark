package structuredstreaming

import org.apache.spark.sql.streaming.{ OutputMode, Trigger }
import global.GlobalClass.spark

/**
  * Not every time you would want to save the state
  * The state is not saved in this example
  * Here the trigger will invoke every 5 second.
  * The processing will be done only if any data is available in the 5 seconds duration
  * Otherwise there will be no data dump on the console
  * This functionality is exactly similar to the batch time feature of DStream
  */
object _4StreamingStatelessWordCount {

  def main(args: Array[String]): Unit = {


    //create stream from socket
    val socketStreamDf = spark .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import spark.implicits._
    val socketDs = socketStreamDf.as[String]
    val wordsDs = socketDs.flatMap(value ⇒ value.split(" "))

    /***
      * Here the aggregation is one (groupBy) after which the
      * Aggregate is converted back to dataframe of word counts
      * Therefore append flag is needed in the writeStream below
      */
    val countDs = wordsDs.groupByKey(value => value).flatMapGroups{
      case (value, iter) ⇒ Iterator((value, iter.length))
    }.toDF("value", "count")


    /***
      * See the append flag ? It is because the aggregate was converted back
      * to dataframe on line 37
      */
    val query =
      countDs.writeStream.format("console").outputMode(OutputMode.Append())
        .trigger(Trigger.ProcessingTime("15 seconds"))

    query.start().awaitTermination()
  }
}
