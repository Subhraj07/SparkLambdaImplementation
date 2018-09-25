package clickstream

import java.io.FileWriter

import config.Settings
import org.apache.commons.io.FileUtils

import scala.util.Random

object LogProducerProducerStream extends App{

  //WebLog Config
  val wlc = Settings.WebLogGen

  val products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray

  val visitors = (0 to wlc.visitors).map("visitors-" + _)
  val pages = (0 to wlc.pages).map("pages-" + _)

  val rnd = new Random()
  val filepath = wlc.filepath
  val destPath = wlc.destpath

  for (filecount <- 1 to wlc.num_of_files)
  {

    val fw = new FileWriter(filepath, true)

    // Some random generation
    val incrementTimeEvery = rnd.nextInt(wlc.records - 1) + 1

    var timeStamp = System.currentTimeMillis()
    var adjustedTimestamp = timeStamp

    for (iteration <- 1 to wlc.records) {

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

      fw.write(lines)

      if (iteration % incrementTimeEvery == 0) {
        //os.flush
        println(s"Sent $iteration messages!")
        val sleeping = rnd.nextInt(incrementTimeEvery * 60)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }

    }

    fw.close()

    val outputFile = FileUtils.getFile(s"${destPath}data_$timeStamp")
    print(s"moving produced data to ${outputFile}")
    FileUtils.moveFile(FileUtils.getFile(filepath),outputFile)
    val sleeping = 5000
    print(s"sleeping for ${sleeping} ms")
  }

}
