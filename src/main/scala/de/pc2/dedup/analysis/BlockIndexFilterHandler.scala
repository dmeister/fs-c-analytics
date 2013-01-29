/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import java.nio.charset.Charset

import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set

import com.google.common.io.Files
import com.google.gson.GsonBuilder

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.chunker.File
import de.pc2.dedup.util.Log
import de.pc2.dedup.DefaultFileDataHandler

/**
 * Configuration for the block-index-filter simulation
 */
class BlockIndexFilterConfig(val dataSet: String,
  val blockSize: Int,
  val blockIndexPageSize: Int,
  val chunkIndexPageSize: Int,
  val diffCacheSize: Int,
  val blockCacheSize: Int,
  val chunkCacheSize: Int,
  val minDiffValue: Int,
  val negativeLookupProbabiliy: Double) {
}

/**
 * Statistics about the block index filter
 */
class BlockIndexFilterStatistics(
  val fileNumber: Int) {

  var chunkCount: Int = 0
  var uniqueChunkCount: Int = 0
  var redundantChunkCount: Int = 0
  var writeCacheHits: Int = 0

  var blockChunkCacheStats: BlockChunkCacheStatistics = null
  var chunkCacheStats: ChunkCacheStatistics = null
  var blockIndexStats: BlockIndexStatistics = null
  var chunkIndexStats: ChunkIndexStatistics = null

  var totalStorageCount: Long = 0
  var totalStorageByteCount: Long = 0

  def finish() {
    blockChunkCacheStats.finish()
    chunkCacheStats.finish()

    blockIndexStats.finish()
    chunkIndexStats.finish()

    totalStorageCount = blockIndexStats.storageCount + chunkIndexStats.storageCount
    totalStorageByteCount = blockIndexStats.storageByteCount + chunkIndexStats.storageByteCount
  }
}

/**
 * Block index filter handler
 */
class BlockIndexFilterHandler(config: BlockIndexFilterConfig, output: Option[String], ioTrace: DiskSimTrace) extends DefaultFileDataHandler with Log {
  val blockIndex = new BlockIndex(config.blockSize, config.blockIndexPageSize, ioTrace)
  val chunkIndex = new ChunkIndex(16 * 1024 * 1024, config.chunkIndexPageSize, config.negativeLookupProbabiliy, ioTrace)
  val blockChunkCache = new BlockChunkCache(config,
    blockIndex)
  val chunkCache = new ChunkCache(config.chunkCacheSize, chunkIndex)

  var currentBlockId = 0
  val currentBlockMappingSet = Set.empty[Digest]
  var currentBlockMappingOffset: Int = 0
  
  var traceFileCount = 0
  val currentFetchSet = Set.empty[Int]

  var blockIdAtStart: Option[Int] = None
  var blockIdAtLastStart: Option[Int] = None

  var currentStats: BlockIndexFilterStatistics = null;
  val stats = new ListBuffer[BlockIndexFilterStatistics]
  val gson = new GsonBuilder().setPrettyPrinting().create()

  logger.info(gson.toJson(config))

  override def beginFile(traceFileNumber: Int) {
    logger.info("Begin file %s (%s)".format(traceFileNumber, traceFileCount))
    traceFileCount += 1
    currentStats = new BlockIndexFilterStatistics(traceFileNumber)

    blockIdAtLastStart = blockIdAtStart
    blockIdAtStart = Some(currentBlockId)
    logger.debug("Block id at start: %s".format(blockIdAtStart))
  }

  override def endFile(traceFileNumber: Int) {
    logger.info("End file %s".format(traceFileNumber))
    
    if (currentBlockMappingSet.size > 0) {
      updateBlock(currentBlockId, currentBlockMappingSet)
      currentBlockId += 1
      currentBlockMappingOffset = 0
      currentBlockMappingSet.clear()
    }

    currentStats.blockChunkCacheStats = blockChunkCache.stats
    currentStats.chunkCacheStats = chunkCache.stats
    currentStats.blockIndexStats = blockIndex.stats
    currentStats.chunkIndexStats = chunkIndex.stats
    currentStats.finish()

    logger.info(gson.toJson(currentStats))
    stats.append(currentStats)

    blockChunkCache.finish()
    chunkCache.finish()
    blockIndex.finish()
    chunkIndex.finish()

    // Do make the simulation easier we clear to block index
    
      if (!blockIdAtLastStart.isEmpty) {
        blockIndex.clear(blockIdAtLastStart.get)
      }
    
    currentStats = null
  }

  def updateBlock(blockId: Int, itemSet: Set[Digest]) {
    // get a new copy
    val bmi = Set.empty[Digest]
    bmi ++= itemSet
    val bm = new BlockMapping(bmi)
    blockIndex.add(blockId, bm)
  }

  def handleFullFile(f: File, chunkList: Seq[Chunk]) {
    logger.debug("Handle file %s".format(f.filename))

    var lastDiff: Int = 0
    for (chunk <- chunkList) {
      currentStats.chunkCount += 1

      // update the block id
      if ((currentBlockMappingOffset + chunk.size) >= config.blockSize) {
        // Finish block
        currentBlockMappingSet += chunk.fp
        updateBlock(currentBlockId, currentBlockMappingSet)
        currentBlockId += 1
        currentBlockMappingSet.clear()

        currentBlockMappingOffset = chunk.size
      } else {
        currentBlockMappingOffset += chunk.size
      }

      // this is similar to a bloom filter (without an error rate)
      chunkIndex(chunk.fp, false) match {
        case Some(cm) =>
          if (chunkCache.contains(chunk.fp)) {
            // ok
            currentStats.redundantChunkCount += 1
            logger.debug("%s: Chunk cache hit".format(currentBlockId, chunk.fp))
          } else if (currentBlockMappingSet.contains(chunk.fp)) {
            // write cache hit
            // I don't care here
            currentStats.writeCacheHits += 1
            currentStats.redundantChunkCount += 1
            logger.debug("%s/%s: Write block cache hit".format(currentBlockId, chunk.fp))
          } else if (blockChunkCache.contains(chunk.fp, currentBlockId)) {
            // ok
            currentStats.redundantChunkCount += 1
            logger.debug("%s/%s: Block cache hit".format(currentBlockId, chunk.fp))
          } else {
            // all caches missed
            chunkIndex(chunk.fp, true) match {
              case Some(cm) =>
                logger.debug("%s/%s: Cache miss: %s".format(currentBlockId, chunk.fp, cm))
                blockChunkCache.update(chunk.fp, currentBlockId, cm)
                currentStats.redundantChunkCount += 1
              case None =>
                throw new Exception("Should not happen")
            }
          }

        case None =>
          // now collect the access
          chunkIndex(chunk.fp, true) match {
            case Some(cm) => throw new Exception("Should not happen")
            case None =>
              currentStats.uniqueChunkCount += 1
              val cm = new ChunkMapping(0)
              cm.markBlock(currentBlockId)
              chunkIndex.add(chunk.fp, cm)

              logger.debug("%s/%s: New chunk: %s".format(currentBlockId, chunk.fp, cm))
          }
      }

      // update gc and block mapping (in memory operation)
      chunkIndex(chunk.fp, false) match {
        case Some(cm) =>
          logger.debug("%s/%s: Mark block: %s".format(currentBlockId, chunk.fp, cm))
          cm.markBlock(currentBlockId)
        //logger.debug("%s/%s: %s".format(currentBlockId, chunk.fp, if (cm.blockSet.size < 16) cm.blockSet else "<too large>"))
        case None => throw new Exception("Failed to find chunk mapping")
      }
      // update chunk cache
      chunkCache.update(chunk.fp)

      currentBlockMappingSet += chunk.fp
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
