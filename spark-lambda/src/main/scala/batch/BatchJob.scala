package batch

import java.lang.management.ManagementFactory
import org.apache.spark.{SparkContext,SparkConf}
import domain._

object BatchJob {

  def main(args: Array[String]): Unit = {

    //get spark conf
    val conf = new SparkConf().setAppName("Lambda With Spark")

    //check if running from IDE
    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      System.setProperty("hadoop.home.dir","D:\\winutils")
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)

    val sourcefile = "file:///D:/subhrajit/personal/pluralsight/SparkComplete/data/data.tsv"
    val input = sc.textFile(sourcefile)

//    input.foreach(println)

    val inputRDD = input.flatMap{ line =>
      val records = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (records.length == 7)
        Some(Activity(records(0).toLong / MS_IN_HOUR * MS_IN_HOUR, records(1),records(2),records(3),records(4),records(5),records(6)))
      else
        None
    }

    val keyByProduct = inputRDD.keyBy(a=> (a.product,a.timestamp_hour)).cache()

    val visitorByProduct = keyByProduct
      .mapValues(a=> a.visitor)
      .distinct()
      .countByKey()

    val activityByProduct = keyByProduct
      .mapValues{ a=>
        a.action match {
          case "purchase" => (1,0,0)
          case "add_to_cart" => (0,1,0)
          case "page_view" => (0,0,1)
        }
      }
      .reduceByKey((a,b)=> (a._1 + b._1, a._2 + b._2, a._3 + b._3))

    visitorByProduct.foreach(println)
    println("--------------------------------------------------------------")
    activityByProduct.foreach(println)

    sc.stop()

  }

}
