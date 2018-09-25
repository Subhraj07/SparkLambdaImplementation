package clickstream

import java.io.FileWriter
import java.util.Properties

import config.Settings

import scala.util.Random
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

object LogProducerKafka extends App{

  //WebLog Config
  val wlc = Settings.WebLogGen

  val products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray

  val visitors = (0 to wlc.visitors).map("visitors-" + _)
  val pages = (0 to wlc.pages).map("pages-" + _)

  val rnd = new Random()

  val topic = wlc.kafkaTopic
  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.188.193.152:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "WebLogProducer")

  val kafkaProducer: Producer[Nothing,String] = new KafkaProducer[Nothing,String](props)
  print(kafkaProducer.partitionsFor(topic))

  for (fileCount <- 1 to wlc.num_of_files){

    val incrementTimeEvery = rnd.nextInt(wlc.records-1) + 1
    var timeStamp = System.currentTimeMillis()
    var adjustedTimestamp = timeStamp

    for (iteration  <- 1 to wlc.records) {

      adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timeStamp) * wlc.timemultiplier)
      timeStamp = System.currentTimeMillis()

      val action = iteration % (rnd.nextInt(200) + 1) match {
        case 0 => "purchase"
        case 1 => "add_to_cart"
        case _ => "page_view"
      }

      val referrer = referrers(rnd.nextInt(referrers.length - 1))

      val prevpage = referrer match {

        case "Internal" => pages(rnd.nextInt(referrer.length - 1))
        case _ => ""
      }

      val visitor = visitors(rnd.nextInt(visitors.length - 1))
      val page = pages(rnd.nextInt(pages.length - 1))
      val product = products(rnd.nextInt(products.length - 1))

      val lines = s"$adjustedTimestamp\t$referrer\t$action\t$prevpage\t$visitor\t$page\t$product\n"
      val producerRecord = new ProducerRecord(topic,lines)
      kafkaProducer.send(producerRecord)


      if (iteration % incrementTimeEvery == 0) {
        //os.flush
        println(s"Sent $iteration messages!")
        val sleeping = rnd.nextInt(incrementTimeEvery * 60)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }

      val sleeping = 2000
      println(s"Sleeping for $sleeping ms")
    }

    kafkaProducer.close()
  }

}
