/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import scala.collection.mutable.Set
import scala.collection.mutable.Map

import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.util.Log

/**
 * Statistics about the block chunk cache usage
 */
class BlockChunkCacheStatistics {
  var hits: Int = 0
  var misses: Int = 0
  var hitRatio: Double = 0.0
  var hitsAfterFetch: Int = 0
  var currentGenerationFetchCount: Int = 0
  var falseBlockIndexMiss: Int = 0
  var invalidDiffCount: Int = 0

  def finish() {
    hitRatio = if (hits + misses == 0) {
      0.0
    } else {
      1.0 * hits / (1.0 * (hits + misses))
    }
  }
}

class BlockChunkCache(config: BlockIndexFilterConfig, blockIndex: BlockIndex) extends Log {
  val cache = new LRU[Int, Int](config.blockCacheSize, blockCacheEvict)
  val diffCache = new LRU[Int, Int](config.diffCacheSize, diffCacheEvict)
  val blockChunkMap = Map.empty[Digest, Set[Int]]

  var stats = new BlockChunkCacheStatistics()
  var firstBlockIdInRun: Int = -1

  private def fetchBlock(blockId: Int) {
    def handleBlockMapping(bmi: (Int, BlockMapping)) {
      val (blockId, blockMapping) = bmi
      for (blockMappingDigest <- blockMapping.itemSet) {
        if (!blockChunkMap.contains(blockMappingDigest)) {
          blockChunkMap += (blockMappingDigest -> Set.empty[Int])
        }
        blockChunkMap(blockMappingDigest) += (blockId)
      }
      cache.update(blockId, 1)
    }
    if (!blockIndex.contains(blockId)) {
      return
    }
    if (blockId >= firstBlockIdInRun) {
      stats.currentGenerationFetchCount += 1
    }
    blockIndex.fetchFullPage(blockId, true).foreach(handleBlockMapping)
  }

  def contains(fp: Digest, currentBlockId: Int): Boolean = {
    if (firstBlockIdInRun == -1) {
      firstBlockIdInRun = currentBlockId
    }

    val r = if (blockChunkMap.contains(fp)) {
      val blockSet = blockChunkMap(fp)
      val diffSet = for (blockId <- blockSet) yield (currentBlockId - blockId)
      logger.debug("%s: Block %s".format(fp, blockSet))

      // Update cache, but we only update entries if they are already existing.
      // No new
      var foundBlockEntry = false
      var foundDiffEntry = false
      for (blockId <- blockSet) {
        if (cache.contains(blockId)) {
          cache.update(blockId, 1)
          foundBlockEntry = true
        }
      }

      if (diffSet.size == 1) {
        for (diff <- diffSet) {
          diffCache.update(diff, diffCache(diff) + 1)
        }
      } else {
        for (diff <- diffSet) {
          if (diffCache.contains(diff)) {
            diffCache.update(diff, diffCache(diff) + 1)
            foundDiffEntry = true
          }
        }
      }

      true
    } else {
      doFetchBlockMappings(fp, currentBlockId)
    }

    if (r) {
      stats.hits += 1
    } else {
      stats.misses += 1
    }
    r
  }

  def update(fp: Digest, currentBlockId: Int, chunkMapping: ChunkMapping) {

    val diff: Int = currentBlockId - chunkMapping.lastBlockId
    diffCache.update(diff, diffCache(diff) + 1)

    logger.debug("%s: last block id %s, diff %s".format(fp,
      chunkMapping.lastBlockId,
      diff))
  }

  def finish() {
    cache.clear()
    diffCache.clear()
    blockChunkMap.clear()

    stats = new BlockChunkCacheStatistics()
    firstBlockIdInRun = -1
  }

  def blockCacheEvict(blockId: Int, usedCount: Int): Boolean = {
    logger.debug("Evicted block %s".format(blockId))

    val blockMapping = blockIndex(blockId, false) match { // dont't count the IOs here
      case Some(bm) => bm
      case None => throw new Exception("Failed to find block mapping")
    }
    for (blockMappingDigest <- blockMapping.itemSet) {
      blockChunkMap(blockMappingDigest) -= (blockId)
      if (blockChunkMap(blockMappingDigest).size == 0) {
        blockChunkMap -= blockMappingDigest
      }
    }

    return true
  }

  def diffCacheEvict(diff: Int, usedCount: Int): Boolean = {
    logger.debug("Evicted diff %s".format(diff))
    return true
  }

  private def doFetchBlockMappings(fp: de.pc2.dedup.chunker.Digest, currentBlockId: Int): Boolean = {
    // not found in current block chunk cache set
    var diffCacheState = false
    for (diffOffset <- diffCache.orderedKeys.reverse) {
      if (!diffCacheState) {
        val checkBlockId = currentBlockId - diffOffset
        if (blockIndex.contains(checkBlockId) && !cache.contains(checkBlockId)) {
          if (blockChunkMap.contains(fp)) {
            diffCache.update(diffOffset, diffCache(diffOffset) + 1)
          }

          if (diffCache.apply(diffOffset) > config.minDiffValue) {
            logger.debug("Fetch block %s, diff %s".format(checkBlockId, diffOffset))
            // Only fetch if we have seen the diff before often enought.
            // The diff has proven to be a reliable value
           fetchBlock(checkBlockId)
            
            if (blockChunkMap.contains(fp)) {
              stats.hitsAfterFetch += 1
              diffCacheState = true
            }
          } else {
            logger.debug("Pass fetching: block %s, diff %s, diff value %s".format(checkBlockId, diffOffset, diffCache(diffOffset)))
          }
        } else {
          // This is a simulation artifact
          if (!blockIndex.contains(checkBlockId) && checkBlockId < currentBlockId) {
            stats.falseBlockIndexMiss += 1
          }
        }
      }
    }
    diffCacheState
  }
}