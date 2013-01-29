/*
 * fsc-analytics
 */
package de.pc2.dedup.compression

import java.nio.charset.Charset

import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

import com.google.common.io.Files
import com.google.gson.GsonBuilder

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.chunker.File
import de.pc2.dedup.util.Log
import de.pc2.dedup.DefaultFileDataHandler

class EntropyHandlerConfig(val dataSet: String) {

}

class EntropyHandlerHandlerStatistics(var fileNumber: Int) {
  var rawChunkBits: Long = 0
  var countingChunkBits: Double = 0
  var chunkCount: Long = 0
  var uniqueChunkCount: Long = 0
  var orderZeroEntropyBits: Long = 0
  var orderOneEntropyBits: Long = 0
  var orderZeroModelSize: Long = 0
  var orderOneModelSize: Long = 0

  def finish() {
  }
}

class EntropyHandler(config: EntropyHandlerConfig, output: Option[String]) extends DefaultFileDataHandler with Log {
  val chunkIndex = Map.empty[Digest, Map[Digest, Int]]

  var currentStats: EntropyHandlerHandlerStatistics = null;
  val stats = new ListBuffer[EntropyHandlerHandlerStatistics]
  val gson = new GsonBuilder().setPrettyPrinting().create()

  var chunkCount: Long = 0
  var traceFileCount = 0
  var lastDigest: Digest = null

  logger.info(gson.toJson(config))

  override def beginFile(traceFileNumber: Int) {
    logger.info("Begin file %s (%s)".format(traceFileNumber, traceFileCount))
    traceFileCount += 1

    currentStats = new EntropyHandlerHandlerStatistics(traceFileNumber)
  }

  override def endFile(traceFileNumber: Int) {

    logger.info("End file %s".format(traceFileNumber))

    val log2 = scala.math.log(2)
    var zeroOrder: Double = 0.0
    var oneOrder: Double = 0.0
    var uniqueChunkCount: Long = 0

    for ((digest, digestMap) <- chunkIndex) {
      var count = 0

      for ((coDigest, coDigestCount) <- digestMap) {
        count += coDigestCount
      }
      for ((coDigest, coDigestCount) <- digestMap) {
        val p = 1.0 * coDigestCount / count
        val e = -1 * (scala.math.log(p) / log2)

        //logger.info(" 1 count %s, total count %s => entropy %s".format(coDigestCount, count, e))
        oneOrder += (e * coDigestCount)
        currentStats.orderOneModelSize += 160
      }

      val p = 1.0 * count / chunkCount
      val e = -1 * (scala.math.log(p) / log2)
      //logger.debug("%s, uc %s, p %f, e %f".format(digest, count, p, e))
      //logger.info("0 count %s, total count %s => entropy %s".format(count, chunkCount, e))
      currentStats.orderZeroModelSize += 160
      zeroOrder += (e * count)

      uniqueChunkCount += 1
    }
    currentStats.orderOneEntropyBits = oneOrder.toInt
    currentStats.orderZeroEntropyBits = zeroOrder.toInt
    currentStats.rawChunkBits = (chunkCount * 160)
    currentStats.countingChunkBits = (chunkCount * (scala.math.log(uniqueChunkCount) / log2))
    currentStats.chunkCount = chunkCount
    currentStats.uniqueChunkCount = uniqueChunkCount
    currentStats.finish()
    logger.info(gson.toJson(currentStats))
    stats.append(currentStats)
    currentStats = null
    lastDigest = null
  }

  def handleFullFile(f: File, chunkList: Seq[Chunk]) {
    logger.debug("Handle file %s".format(f.filename))
    for (chunk <- chunkList) {
      chunkCount += 1

      if (!chunkIndex.contains(lastDigest)) {
        chunkIndex += (lastDigest -> Map.empty[Digest, Int])
      }
      val chunkMap: Map[Digest, Int] = chunkIndex(lastDigest)

      if (chunkMap.contains(chunk.fp)) {
        val oldCount: Int = chunkMap(chunk.fp)
        val newCount: Int = oldCount + 1
        chunkMap += (chunk.fp -> newCount)
      } else {
        chunkMap += (chunk.fp -> 1)
      }

      lastDigest = chunk.fp
    }
  }

  override def quit() {
    val statistics = gson.toJson(stats.asJava)
    logger.info(statistics)
    output match {
      case Some(filename) =>
        Files.write(statistics, new java.io.File(filename), Charset.forName("UTF-8"))
      case _ =>
    }
  }
}
