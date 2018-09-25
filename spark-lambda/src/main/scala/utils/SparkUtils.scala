package utils

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{StreamingContext,Duration}
import org.apache.spark.sql.SQLContext



object SparkUtils {

  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  var checkPointDir = ""

  def getSparkContext(appName: String) ={

    //get spark conf
    val conf = new SparkConf().setAppName(appName)

    //check if running from IDE
    if (isIDE) {
      System.setProperty("hadoop.home.dir","D:\\winutils")
      conf.setMaster("local[*]")
      checkPointDir = "file:///D:/subhrajit/personal/pluralsight/SparkComplete/checkpoint"
    }
    else
      checkPointDir = "hdfs://10.188.193.152:8022/user/sumohanty/spark_test/checkpoint"

    //    for debug
    //        System.setProperty("hadoop.home.dir","D:\\winutils")
    //        conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    // If sc already exists
    // val sc = SparkContext.getOrCreate(conf)

    sc.setCheckpointDir(checkPointDir)
    sc
  }

  def getSqlContext(sc : SparkContext) ={

    implicit val sqlContext = new SQLContext(sc)
    sqlContext
  }

  def getStreamingContext(streamingApp: (SparkContext,Duration) => StreamingContext,sc : SparkContext, batchDuration: Duration) ={
    def createFunction = () => streamingApp(sc,batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkPointDir) => StreamingContext.getActiveOrCreate(checkPointDir,createFunction,sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(createFunction)
    }

    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }

}
