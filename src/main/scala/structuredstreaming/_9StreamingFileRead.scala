package structuredstreaming

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import global.GlobalClass.spark

/**
  * Lets us now see the capability of streaming data from files
  * Something like a location where new files will keep coming
  * The data will be read as and when the file comes.
  */
object _9StreamingFileRead {

  def main(args: Array[String]): Unit = {

    val schema = StructType(
      Array(StructField("transactionId", StringType),
        StructField("customerId", StringType),
        StructField("itemId", StringType),
        StructField("amountPaid", StringType)))

    //create stream from folder
    val fileStreamDf = spark.readStream
      .option("header", "true")
      .schema(schema)
      .csv("/Users/syed/Desktop/streaming/filestreaming")

    val query = fileStreamDf.writeStream
      .format("console")
      .outputMode(OutputMode.Append()).start()

    query.awaitTermination()

  }

}

