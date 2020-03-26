package structuredstreaming

import global.GlobalClass.spark
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}

object _10StreamingCheckpointing {

  def main(args: Array[String]): Unit = {


    val schema = StructType(
      Array(StructField("transactionId", StringType),
        StructField("customerId", StringType),
        StructField("itemId", StringType),
        StructField("amountPaid", DoubleType)))

    //create stream from folder
    val fileStreamDf = spark.readStream
      .option("header", "true")
      .schema(schema)
      .csv("/Users/syed/Desktop/streaming/filestreaming")

    val countDs = fileStreamDf.groupBy("customerId").sum("amountPaid")
    val query =
      countDs.writeStream
        .format("console")
        .option("checkpointLocation", "/Users/syed/Desktop/streaming/checkpointing")
        .outputMode(OutputMode.Complete())

    query.start().awaitTermination()

    //Now put this on netcat and enter :  111,1,1,100.0
    //You will get the data for customerid 1
    //Now try another customer id : 111,3,1,100.0
  }
}

