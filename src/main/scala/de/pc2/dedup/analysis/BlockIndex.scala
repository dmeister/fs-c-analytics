/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import scala.collection.mutable.Map
import scala.collection.mutable.Set

import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.util.Log

/**
 * Model of a block mapping
 */
class BlockMapping(var itemSet: Set[Digest]) {
}

/**
 * Model of a block mapping with the items stored as a list instead of a set
 *
 */
class BlockMappingList(var items: List[Digest]) {
}

/**
 * Statistics about the block index usage.
 */
class BlockIndexStatistics {
  var storageCount: Long = 0
  var storageByteCount: Long = 0
  var fetchCount: Int = 0
  var fetchChunkCount: Int = 0

  def finish() {
  }
}

/**
 * Model of the block index
 */
class BlockIndex(blockSize: Int, pageSize: Int, ioTrace: DiskSimTrace) extends Log {
  val index = Map.empty[Int, BlockMapping]
  val blockIndexPageCache = new LRU[Int, Int](16)

  /**
   * Remove all blocks with a block id less then the given block id
   * This is used to avoid holding a block index in memory.
   */
  def clear(blockId: Int) {

    val keyList = index.keys.toList
    for (key <- keyList) {
      if (key < blockId) {
        index -= key
      }
    }
  }

  def add(blockId: Int, bm: BlockMapping) {
    index += (blockId -> bm)
  }

  var stats = new BlockIndexStatistics()

  def finish() {
    stats = new BlockIndexStatistics()
  }

  def getPageForBlock(blockId: Int): Int = {
    val averageChunksPerBlock = (blockSize / (8 * 1024))
    val estimatedBlockMappingSize = averageChunksPerBlock * 24
    val blockMappingsPerPage = pageSize / estimatedBlockMappingSize
    return blockId / blockMappingsPerPage;
  }

  def contains(blockId: Int): Boolean = {
    return index.contains(blockId)
  }

  def fetchFullPage(blockId: Int, countIO: Boolean): List[(Int, BlockMapping)] = {
    def fetchIfOnePage(fetchBlockid: Int, page: Int, diff: Int): List[(Int, BlockMapping)] = {
      if (getPageForBlock(fetchBlockid) != page) Nil
      else if (!index.contains(fetchBlockid)) {
        fetchIfOnePage(fetchBlockid + diff, page, diff)
      } else {
        val bm = index(fetchBlockid)
        stats.fetchCount += 1
        stats.fetchChunkCount += bm.itemSet.size
        (fetchBlockid, bm) :: fetchIfOnePage(fetchBlockid + diff, page, diff)
      }
    }
    val indexPage = getPageForBlock(blockId)
    if (countIO) {
      if (!blockIndexPageCache.contains(indexPage)) {
        blockIndexPageCache.update(indexPage, 0)
        ioTrace.record(0, 1, indexPage * pageSize, pageSize, 1)

        stats.storageCount += 1
        stats.storageByteCount += pageSize
      }
    }
    val bml = index.get(blockId) match {
      case None => Nil
      case Some(bm) =>
        stats.fetchCount += 1
        stats.fetchChunkCount += bm.itemSet.size
        List((blockId, bm))
    }
    bml ::: fetchIfOnePage(blockId - 1, indexPage, -1) ::: fetchIfOnePage(blockId + 1, indexPage, +1)
  }

  def apply(blockId: Int, countIO: Boolean): Option[BlockMapping] = {
    if (countIO) {
      val indexPage = getPageForBlock(blockId)
      if (!blockIndexPageCache.contains(indexPage)) {
        blockIndexPageCache.update(indexPage, 0)
        ioTrace.record(0, 1, indexPage * pageSize, pageSize, 1)

        stats.storageCount += 1
        stats.storageByteCount += pageSize
      }

    }
    if (!index.contains(blockId)) {
      None
    } else {
      val bm = index(blockId)
      stats.fetchCount += 1
      stats.fetchChunkCount += bm.itemSet.size

      Some(bm)
    }
  }
}