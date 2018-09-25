package Streaming

import domain.{Activity, ActivityByProduct}
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import utils._
import functions._

object StreamingJobDFMapStateByKey {

  def main(args: Array[String]): Unit = {

    val sc = SparkUtils.getSparkContext("Streaming with spark")
    val sqlContext = SparkUtils.getSqlContext(sc)
    import sqlContext.implicits._

    val batchDuration = Seconds(4)

    def streamingApp (sc: SparkContext, batchDuration: Duration): StreamingContext= {

      val ssc = new StreamingContext(sc,batchDuration)

      val inputPath = SparkUtils.isIDE match {
        case true => "file:///D:/subhrajit/personal/pluralsight/SparkComplete/input"
        case false => "hdfs://10.188.193.152:8022/user/sumohanty/spark_test/input"
      }

      val textDstream = ssc.textFileStream(inputPath)
      val activittStream = textDstream.transform{input=>
        input.flatMap{ line =>
          val records = line.split("\\t")
          val MS_IN_HOUR = 1000 * 60 * 60
          if (records.length == 7)
            Some(Activity(records(0).toLong / MS_IN_HOUR * MS_IN_HOUR, records(1),records(2),records(3),records(4),records(5),records(6)))
          else
            None
        }
      }

     val activityStateSpec =
        StateSpec
          .function(mapActivityStateFunc)
          .timeout(Seconds(30))

      val statefulActivityByProduct = activittStream.transform(rdd => {
        val df = rdd.toDF()
        df.registerTempTable("activity")
        val activityByProduct = sqlContext.sql(
          """
            |select product,timestamp_hour,
            |sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
            |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
            |sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
            |from activity
            |group by product,timestamp_hour
          """.stripMargin)

        activityByProduct.map{r => ((r.getString(0),r.getLong(1)),
          ActivityByProduct(r.getString(0),r.getLong(1),r.getLong(2),r.getLong(3),r.getLong(4))
          )}
      }).mapWithState(activityStateSpec)



      ssc
    }

    val ssc = SparkUtils.getStreamingContext(streamingApp,sc,batchDuration)

    ssc.start()
    ssc.awaitTermination()

  }
}
