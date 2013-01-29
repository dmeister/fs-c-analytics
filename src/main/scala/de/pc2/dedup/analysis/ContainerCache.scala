/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import scala.collection.mutable.Map

import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.util.Log

class ContainerCacheStatistics {
  var hits: Int = 0
  var misses: Int = 0
  var hitRatio: Double = 0.0
  var fetchCount: Int = 0

  def finish() {
    hitRatio = if (hits + misses == 0) {
      0.0
    } else {
      1.0 * hits / (1.0 * (hits + misses))
    }
  }
}

/**
 * A container cache simulation used to simulate the approach of DDFS
 */
class ContainerCache(containerCacheSize: Int, containerStorage: ContainerStorage) extends Log {
  val cache = new LRU[Int, Int](containerCacheSize, containerCacheEvict)
  var stats = new ContainerCacheStatistics()
  val chunkMap = Map.empty[Digest, Int]

  def finish() {
    cache.clear()
    chunkMap.clear()

    stats = new ContainerCacheStatistics()
  }
  
  def containsContainer(containerId: Int) : Boolean = {
    cache.contains(containerId)
  }

  def contains(fp: Digest): Boolean = {
    if (chunkMap.contains(fp)) {
      val containerId = chunkMap(fp)
      cache.update(containerId, 0)

      logger.debug("Found chunk %s: container %s".format(fp, containerId))
      stats.hits += 1
      true
    } else {
      stats.misses += 1
      false
    }
  }

  def containerCacheEvict(containerId: Int, usedCount: Int): Boolean = {
    logger.debug("Evict container %s".format(containerId))

    containerStorage.readContainer(containerId, false) match {
      case Some(chunks) =>
        // remove chunk from chunk cache
        for (chunk <- chunks) {
          chunkMap -= chunk
        }
      case None => throw new Exception("Failed to find container")
    }
    true
  }

  def update(containerId: Int) {
    stats.fetchCount += 1
    val chunks = containerStorage.readContainer(containerId, true) match {
      case Some(chunks) =>
        for (chunk <- chunks) {
          chunkMap += (chunk -> containerId)
        }
        cache.update(containerId, 0)
      case None =>
    }
  }
}