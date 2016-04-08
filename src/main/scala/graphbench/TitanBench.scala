package graphbench

import java.io.File

import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph
import graphbench.Util._
import org.apache.commons.io.FileUtils
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory

import scala.collection.JavaConversions._

object TitanBench extends App {
  val tuples = autoClosing(loadClickstream()) { data =>
    val lines = data.getLines()
    lines.next() // skip header

    var cnt = 0
    val tuples = lines.take(100000).map(_.split("\t")).map {
      case Array(prev, curr, tpe, n) =>
        cnt += 1
        if (cnt % 8192 == 0) {
          print(s"\rpreloaded $cnt records")
        }
        (sanitize(prev), sanitize(curr), tpe, n.toInt)
    }.toSeq

    println(s"\r${tuples.size} records loaded")
    tuples
  }

  val file = new File("db")
  FileUtils.deleteQuietly(file)
  file.mkdirs()

  // schema creation
  autoClosing(GraphFactory.open(Map(
    "gremlin.graph" -> "com.thinkaurelius.titan.core.TitanFactory",
    "storage.backend" -> "hbase",   // or berkeleyje
    "storage.hostname" -> "127.0.0.1",
    "storage.directory" -> "db"
  )).asInstanceOf[StandardTitanGraph]) { graph =>
    val mgmt = graph.openManagement()

    mgmt.makePropertyKey("title").dataType(classOf[String]).make()
    mgmt.makePropertyKey("n").dataType(classOf[Integer]).make()
    mgmt.makeEdgeLabel("other").make()
    mgmt.makeEdgeLabel("link").make()
    mgmt.makeEdgeLabel("external").make()

    mgmt.commit()
  }

  // bulk insert
  autoClosing(GraphFactory.open(Map(
    "storage.batch-loading" -> "true",
    "gremlin.graph" -> "com.thinkaurelius.titan.core.TitanFactory",
    "storage.backend" -> "hbase",   // or berkeleyje
    "storage.hostname" -> "127.0.0.1",
    "storage.directory" -> "db"
  ))) { graph =>
    println(graph.getClass)
    println(graph.vertices().size)

    val started = System.currentTimeMillis()
    var cnt = 0

    tuples.foreach {
      case (prev, curr, tpe, n) =>
        graph.vertices()
        val v1 = graph.addVertex("title", prev)
        val v2 = graph.addVertex("title", curr)
        v1.addEdge(tpe, v2, "n", new Integer(n))
        cnt += 1
        if (cnt % 100 == 0) {
          println(s"$cnt\t${System.currentTimeMillis() - started}")
        }

    }

    val finished = System.currentTimeMillis()
    val elapsed = (finished - started) / 1000.0
    val size = graph.vertices().size
    println(f"Inserted $size vertices, elapsed : $elapsed sec, ${size/elapsed}%.3f vertices/sec")

  }
}
