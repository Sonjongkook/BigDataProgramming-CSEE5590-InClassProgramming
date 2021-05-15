import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("LogStreaming")
    val ssc = new StreamingContext(conf, Seconds(5))
    val data = ssc.textFileStream("file:///C:/Users/sjk37/Desktop/Source Code/spark-streaming/log")

    //    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    //    val words = lines.flatMap(_.split(" "))
    val lines = data.flatMap(_.split(" "))
    val words = lines.map(word => (word, 1))
    val counts = words.reduceByKey(_ + _)
    counts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
