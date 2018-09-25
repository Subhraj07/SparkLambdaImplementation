package batch


import org.apache.spark.sql.{SaveMode}
import domain._
import utils._

object BatchJobSparkDF {

  def main(args: Array[String]): Unit = {

  // setup sparkcontext
    val sc = SparkUtils.getSparkContext("Lambda with Spark")
    val sqlContext = SparkUtils.getSqlContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    //val sourcefile = "file:///D:/subhrajit/personal/pluralsight/SparkComplete/data/data.tsv"

    //spark-submit
    val sourcefile = "hdfs://10.188.193.152:8022/user/sumohanty/spark_test/data.tsv"
    val input = sc.textFile(sourcefile)

    //input.foreach(println)

    val inputDF = input.flatMap{ line =>
      val records = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (records.length == 7)
        Some(Activity(records(0).toLong / MS_IN_HOUR * MS_IN_HOUR, records(1),records(2),records(3),records(4),records(5),records(6)))
      else
        None
    }.toDF()

    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour")/ 1000),1).as("timestamp_hour"),
      inputDF("referrer"),inputDF("action"),inputDF("prevpage"),inputDF("page"),inputDF("visitor"),inputDF("product")
    )

    df.registerTempTable("activity")

    val visitorByProduct = sqlContext.sql(
      """
        |select product,timestamp_hour,count(distinct(visitor)) as unique_visitors
        |from activity
        |group by product, timestamp_hour
      """.stripMargin)

    visitorByProduct.printSchema()

    val activityByProduct = sqlContext.sql(
      """
        |select product,timestamp_hour,
        |sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
        |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
        |sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
        |from activity
        |group by product,timestamp_hour
      """.stripMargin).cache()

    sqlContext.udf.register("UnderExposed",(pageviewcount:Long,purchasecount:Long) => if (purchasecount==0) 0 else pageviewcount/purchasecount)

    activityByProduct.registerTempTable("activityByProduct")

    val underExposedProduct = sqlContext.sql(
      """
        |select product,timestamp_hour,
        |UnderExposed(page_view_count,purchase_count) as negetive_exposuer
        |from activityByProduct
        |order by negetive_exposuer DESC
        |limit 5
      """.stripMargin)


//    visitorByProduct.foreach(println)
//    println("--------------------------------------------------------------")
//    activityByProduct.foreach(println)

//    activityByProduct.coalesce(2).write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://10.188.193.152:8022/user/sumohanty/spark_test/batch1")
    activityByProduct.coalesce(2).write.mode(SaveMode.Append).parquet("hdfs://10.188.193.152:8022/user/sumohanty/spark_test/batch2")

    sc.stop()

  }

}
