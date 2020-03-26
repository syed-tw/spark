package structuredstreaming


import org.apache.spark.sql.streaming.OutputMode
import global.GlobalClass.spark


/**
  * This is stateful streaming example unlike Dstream
  * In Dstream API, everything was stateless
  */
object _3StreamingSocketWordCount {

  def main(args: Array[String]): Unit = {


    //create stream from socket. This acts like  source, the socket in this case
    val socketStreamDf = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    //This will help on converting data from dataframe to dataset by providing relevant encoders
    import spark.implicits._

    val socketDs = socketStreamDf.as[String]

    val wordsDs =  socketDs.flatMap(word => word.split(" ")).toDF("words")
    //wordsDs.printSchema()
    //Aggregation code here below
    val countDs = wordsDs.groupBy("words").count()
    //countDs.printSchema()

    //Since the script has aggregation, the output method is used as complete here
    val query =
      countDs.writeStream.format("console").outputMode(OutputMode.Complete())

    query.start().awaitTermination()
  }
}
