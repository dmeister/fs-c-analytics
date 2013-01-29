/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import java.nio.charset.Charset

import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ListBuffer

import com.google.common.io.Files
import com.google.gson.GsonBuilder

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.File
import de.pc2.dedup.util.Log
import de.pc2.dedup.DefaultFileDataHandler

class ContainerCacheConfig(val dataSet: String,
  val containerSize: Int,
  val cacheSize: Int,
  val chunkCacheSize: Int,
  val chunkIndexPageSize: Int,
  val negativeLookupProbabiliy: Double) {

}

class ContainerCacheHandlerStatistics(var fileNumber: Int) {
  var chunkCount: Int = 0
  var uniqueChunkCount: Int = 0
  var redundantChunkCount: Int = 0

  var chunkIndexStats: ChunkIndexStatistics = null
  var containerStorageStats: ContainerStorageStatistics = null
  var chunkCacheStats: ChunkCacheStatistics = null
  var containerCacheStats: ContainerCacheStatistics = null

  var totalStorageCount: Long = 0
  var totalStorageByteCount: Long = 0

  def finish() {
    chunkIndexStats.finish()
    containerStorageStats.finish()
    chunkCacheStats.finish()
    containerCacheStats.finish()

    totalStorageCount = containerStorageStats.storageCount + chunkIndexStats.storageCount
    totalStorageByteCount = containerStorageStats.storageByteCount + chunkIndexStats.storageByteCount
  }
}

class ContainerCacheHandler(config: ContainerCacheConfig, output: Option[String], ioTrace: DiskSimTrace) extends DefaultFileDataHandler with Log {
  val chunkIndex = new ChunkIndex(16 * 1024 * 1024, config.chunkIndexPageSize, config.negativeLookupProbabiliy, ioTrace)
  val containerStorage = new ContainerStorage(config.containerSize, ioTrace)

  val containerCache = new ContainerCache(config.cacheSize, containerStorage)
  val chunkCache = new ChunkCache(config.chunkCacheSize, chunkIndex)

  var currentStats: ContainerCacheHandlerStatistics = null;
  val stats = new ListBuffer[ContainerCacheHandlerStatistics]
  val gson = new GsonBuilder().setPrettyPrinting().create()

  var traceFileCount = 0

  logger.info(gson.toJson(config))

  override def beginFile(traceFileNumber: Int) {
    logger.info("Begin file %s (%s)".format(traceFileNumber, traceFileCount))
    traceFileCount += 1

    currentStats = new ContainerCacheHandlerStatistics(traceFileNumber)
  }

  override def endFile(traceFileNumber: Int) {
    logger.info("End file %s".format(traceFileNumber))

    containerStorage.finishContainer()

    currentStats.containerCacheStats = containerCache.stats
    currentStats.chunkCacheStats = chunkCache.stats
    currentStats.containerStorageStats = containerStorage.stats
    currentStats.chunkIndexStats = chunkIndex.stats
    currentStats.finish()
    logger.info(gson.toJson(currentStats))
    stats.append(currentStats)
    currentStats = null

    chunkIndex.finish()
    containerStorage.finish()
    chunkCache.finish()
    containerCache.finish()
  }

  def handleFullFile(f: File, chunkList: Seq[Chunk]) {
    logger.debug("Handle file %s".format(f.filename))
    for (chunk <- chunkList) {
      currentStats.chunkCount += 1

      // this is similar to a bloom filter (without an error rate)
      chunkIndex(chunk.fp, false) match {
        case None =>
          // now we count the access
          chunkIndex(chunk.fp, true) match {
            case Some(cm) => throw new Exception("Should not happen")
            case None =>
              currentStats.uniqueChunkCount += 1

              val containerId = containerStorage.addToCurrentContainer(chunk.fp, chunk.size)

              val chunkMapping = new ChunkMapping(containerId)
              chunkIndex.add(chunk.fp, chunkMapping)
          }
        case Some(cm) =>
          if (chunkCache.contains(chunk.fp)) {
            // chunk cache hit
            currentStats.redundantChunkCount += 1
          } else if (containerCache.contains(chunk.fp)) {
            // container cache hit
            currentStats.redundantChunkCount += 1
          } else {
            chunkIndex(chunk.fp, true) match {
              case Some(cm) =>
                currentStats.redundantChunkCount += 1

                containerCache.update(cm.containerId)
              case None =>
                throw new Exception("Should not happen")
            }
          }

      }
      chunkCache.update(chunk.fp)
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
