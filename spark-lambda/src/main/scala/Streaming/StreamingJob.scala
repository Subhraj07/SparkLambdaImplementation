package Streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils._

object StreamingJob {

  def main(args: Array[String]): Unit = {

    val sc = SparkUtils.getSparkContext("Streaming with spark")

    val batchDuration = Seconds(4)

    def streamingApp (sc: SparkContext, batchDuration: Duration): StreamingContext= {

      val ssc = new StreamingContext(sc,batchDuration)

      val inputPath = SparkUtils.isIDE match {
        case true => "file:///D:/subhrajit/personal/pluralsight/SparkComplete/input"
        case false => "hdfs://10.188.193.152:8022/user/sumohanty/spark_test/input"
      }

      val textDstream = ssc.textFileStream(inputPath)
      textDstream.print()

      ssc

    }

    val ssc = SparkUtils.getStreamingContext(streamingApp,sc,batchDuration)

    ssc.start()
    ssc.awaitTermination()

  }
}
