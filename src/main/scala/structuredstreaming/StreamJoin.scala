package structuredstreaming



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import global.GlobalClass.spark


/**
  * Lets create a join on streaming data.
  * This can be useful in enriching data that comes in the stream
  * Suppose only the customer id is coming in the stream.
  * We can enrich it by joining it with Customer Dataset to provide the customer name
  */
object StreamJoin {

  //Two case classes defined
  case class Sales(
                    transactionId: String,
                    customerId:    String,
                    itemId:        String,
                    amountPaid:    Double)
  case class Customer(customerId: String, customerName: String)


  def main(args: Array[String]): Unit = {

    import spark.implicits._
    //take customer data as static df
    val customerDS = spark.read
      .format("csv")
      .option("header", true)
      .load("src/main/resources/customers.csv")
      .as[Customer]

    customerDS.show()

    //create stream from socket
    val socketStreamDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()


    //Read the sales data from the socket
    val dataDf = socketStreamDF.as[String].flatMap(value ⇒ value.split(" "))

    //Create the sales dataset from the dataDF
    val salesDS = dataDf
      .as[String]
      .map(value ⇒ {
        val values = value.split(",")
        Sales(values(0), values(1), values(2), values(3).toDouble)
      })


    //Join the data between both the datasets
    val joinedDs = salesDS
      .join(customerDS, "customerId")

    //Prepare the query by writing it to the stream
    val query =
      joinedDs.writeStream.format("console").outputMode(OutputMode.Append())

    //Start the stream
    query.start().awaitTermination()

  }

}



