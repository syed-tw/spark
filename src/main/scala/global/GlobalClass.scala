package global

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object GlobalClass {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("Spark-Batch-Streaming-Analytics")
    .master("local")
    .getOrCreate()



}
