package graphbench

import java.io.{BufferedInputStream, FileInputStream}
import java.net.URL
import java.nio.file.{Paths, Files}
import java.util.zip.GZIPInputStream

import org.slf4j.{LoggerFactory, Logger}

import scala.io.Source
import scala.language.reflectiveCalls

object Util {
  val logger = LoggerFactory.getLogger(getClass)
  val url = "https://s3-eu-west-1.amazonaws.com/pfigshare-u-files/4901611/2016_03_clickstream.tsv.gz"
  val filename = url.split("/").last
  val path = Paths.get(filename)

  def loadClickstream(): Source = {
    if (!Files.exists(Paths.get(filename))) {
      logger.info(s"Downloading data from $url...")
      val conn = new URL(url).openConnection()
      val length = conn.getContentLengthLong
      val is = conn.getInputStream
      val os = Files.newOutputStream(path)
      var written = 0L
      val buffer = new Array[Byte](8192)
      while (written < length) {
        val read = is.read(buffer)
        os.write(buffer, 0, read)
        written += read
        print(s"\rDownloading ${written / 1024} / ${length / 1024} KB ...")
      }
      println(s"\rDownloaded ${length / 1024} KB")
      is.close()
      os.close()
    }
    Source.fromInputStream(new GZIPInputStream(new BufferedInputStream(Files.newInputStream(path), 8192)))
  }

  def sanitize(token: String): String = {
    token.replaceAll("[{}\"\u001e]", "")
  }

  def autoClosing[R <: { def close(): Unit }, T](resource: R)(closure: R => T): T = {
    try {
      closure(resource)
    } finally {
      resource.close()
    }
  }

}
