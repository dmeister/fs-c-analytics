/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import scala.collection.mutable.Map
import scala.collection.mutable.Set

import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.util.Log

/**
 * Model of a container
 */
case class Container(chunks: Set[Digest]) {

}

/**
 * Statistics again
 */
class ContainerStorageStatistics {
  var containerCount: Int = 0
  var storageCount: Long = 0
  var storageByteCount: Long = 0

  def finish() {
  }
}

/**
 * Model of the container storage
 */
class ContainerStorage(containerSize: Int, ioTrace: DiskSimTrace) extends Log {
  var containerStorage = Map.empty[Int, Container]

  var currentContainerId = 0
  var currentContainerChunks = Set.empty[Digest]
  var freeSpace = containerSize

  var stats = new ContainerStorageStatistics()

  def finish() {
    stats = new ContainerStorageStatistics()
  }

  def finishContainer() {
    if (freeSpace != containerSize) {
      containerStorage += (currentContainerId -> Container(currentContainerChunks))
      currentContainerChunks = Set.empty[Digest]
      stats.containerCount += 1
      freeSpace = containerSize
    }
    currentContainerId += 1
  }

  def addToCurrentContainer(fp: Digest, chunkSize: Int): Int = {
    if (freeSpace < chunkSize) {
      finishContainer()
    }
    currentContainerChunks.add(fp)
    freeSpace -= chunkSize

    currentContainerId
  }

  def getStartPageForContainer(containerId: Int): Int = {
    return containerId * containerSize / (4 * 1024)
  }

  private def roundUpTo(v: Int, v2: Int): Int = {
    val d = v / v2
    val r = v % v2
    if (r == 0) {
      return v
    } else {
      return (d + 1) * v2
    }
  }

  def sizeOfContainerMetadata(): Int = {
    val chunksPerContainer = containerSize / (8 * 1024)
    val metaDataSizePerChunk = 24
    roundUpTo(chunksPerContainer * metaDataSizePerChunk, 4 * 1024)
  }

  def readContainer(containerId: Int, countIO: Boolean): Option[Set[Digest]] = {
    if (!containerStorage.contains(containerId)) {
      None
    } else {
      if (countIO) {
        logger.debug("Read container %s".format(containerId))

        val indexPage = getStartPageForContainer(containerId)
        val requestSize = sizeOfContainerMetadata()
        ioTrace.record(0, 3, getStartPageForContainer(containerId), requestSize, 1)

        stats.storageCount += 1
        stats.storageByteCount += requestSize
      }
      Some(containerStorage(containerId).chunks)
    }
  }
}