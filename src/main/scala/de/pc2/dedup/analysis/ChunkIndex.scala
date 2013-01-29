/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import scala.collection.mutable.Map
import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.util.Log
import scala.util.Random
import de.pc2.dedup.util.LRUMap2

/**
 * Model of a chunk mapping
 */
case class ChunkMapping(var containerId: Int) {
  def this() = this(0)
  var lastBlockId: Int = -1
  var usageCount: Long = 0

  var topNextChunk: Option[Digest] = None

  def markBlock(blockId: Int) {
    lastBlockId = blockId

    usageCount += 1
  }

  override def toString = {
    "[cid %s, last block %s, uc %s]".format(containerId, lastBlockId, usageCount)
  }
}

/**
 * Chunk index statistics
 */
class ChunkIndexStatistics {
  var lookupCount: Int = 0
  var updateCount: Int = 0
  var storageCount: Long = 0
  var storageByteCount: Long = 0
  var chunkCount = 0L

  def finish() {
  }
}

/**
 * Chunk index model for the simulation
 */
class ChunkIndex(indexPageCount: Int, pageSize: Int, negativeLookupProbabiliy: Double, ioTrace: DiskSimTrace) extends Log {
  val index = Map.empty[Digest, ChunkMapping]
  var stats = new ChunkIndexStatistics()
  var r = new Random

  def finish() {
    stats = new ChunkIndexStatistics()
  }

  def add(fp: Digest, cm: ChunkMapping) {
    stats.updateCount += 1
    index += (fp -> cm)
  }

  def getPageForDigest(fp: Digest): Int = {
    return scala.math.abs(fp.hashCode) % indexPageCount
  }

  /**
   * query the chunk index.
   * If countIO is false, the IO is not recorded. This makes the simulation a bit easier.
   */
  def apply(fp: Digest, countIO: Boolean): Option[ChunkMapping] = {
    val result = if (!index.contains(fp)) {
      None
    } else {
      Some(index(fp))
    }

    if (countIO) {
      if (!result.isEmpty || r.nextDouble() < negativeLookupProbabiliy) {
        // count lookup if it is found or when the bloom filter is a false negative
        val indexPage = getPageForDigest(fp)
        ioTrace.record(0, 2, indexPage * pageSize, pageSize, 1)
        stats.storageCount += 1
        stats.storageByteCount += pageSize
      }

      stats.lookupCount += 1
    }

    result
  }
}