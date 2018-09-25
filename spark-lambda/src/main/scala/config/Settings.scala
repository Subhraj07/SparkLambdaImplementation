package config

/*
    Create By Subhrajit Mohanty  on 22-04-2018
 */

import com.typesafe.config.ConfigFactory

object Settings {

  private val config = ConfigFactory.load()

  object WebLogGen{
    private val webLogGen = config.getConfig("clickstream")

    lazy val records = webLogGen.getInt("records")
    lazy val timemultiplier = webLogGen.getInt("time_multiplier")
    lazy val pages = webLogGen.getInt("pages")
    lazy val visitors = webLogGen.getInt("visitors")
    lazy val filepath = webLogGen.getString("filepath")
    lazy val destpath = webLogGen.getString("destpath")
    lazy val num_of_files = webLogGen.getInt("num_of_files")
    lazy val kafkaTopic = webLogGen.getString("kafkaTopic")

  }

}
