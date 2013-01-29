/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import java.nio.charset.Charset

import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.Set

import com.google.common.io.Files
import com.google.common.primitives.UnsignedBytes
import com.google.gson.GsonBuilder

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.chunker.DigestFactory
import de.pc2.dedup.chunker.File
import de.pc2.dedup.util.Log
import de.pc2.dedup.DefaultFileDataHandler

/**
 * Model of a extreme binning bin
 */
class Bin(val fullFileFingerprint: Digest, val chunks: Set[Digest]) {

  def onDiskSize(): Int = {
    (chunks.size * 24)
  }
}

/**
 * Bin index statistics
 */
class BinIndexStatistics() {
  var lookupCount: Int = 0
  var updateCount: Int = 0
  var storageCount: Long = 0
  var storageByteCount: Long = 0
  var primaryLookupCount: Int = 0

  var binCount: Int = 0
  var binItemCount: Int = 0
  var averageBinCount: Double = 0.0

  var cacheHitCount: Int = 0
  var cacheMissCount: Int = 0

  def finish() {
  }
}

/**
 * Extreme Binning index model for the simulation
 */
class BinIndex(indexPageCount: Int, config: ExtremeBinningConfig, ioTrace: DiskSimTrace) extends Log {
  /** Mapping from the representative fingerprint to the bin */
  val index = Map.empty[Digest, Bin]

  var stats = new BinIndexStatistics()

  val cache = new LRU[Digest, Int](config.cacheSize, binCacheEvict)

  def finish(): BinIndexStatistics = {

    // get average bin size

    for ((d, b) <- index) {
      stats.binCount += 1
      stats.binItemCount += b.chunks.size
    }
    stats.averageBinCount = if (stats.binCount == 0) {
      Double.NaN
    } else {
      1.0 * stats.binItemCount / stats.binCount
    }
    val previousStats = stats

    stats = new BinIndexStatistics()
    previousStats
  }

  def binCacheEvict(resentatativeFingerprint: Digest, usedCount: Int): Boolean = {
    logger.debug("Evict bin %s".format(resentatativeFingerprint))
    true
  }

  def add(resentatativeFingerprint: Digest, fullFileFingerprint: Digest, chunks: Set[Digest]) {
    stats.updateCount += 1
    index += (resentatativeFingerprint -> new Bin(fullFileFingerprint, chunks))
  }

  def getPageForDigest(fp: Digest): Int = {
    return scala.math.abs(fp.hashCode) % indexPageCount
  }

  /**
   * asks the bin for for a full file fingerprint entry.
   * We assume this is an in-memory operation
   */
  def getFullFileFingerprint(resentatativeFingerprint: Digest): Option[Digest] = {
    if (!index.contains(resentatativeFingerprint)) {
      None
    } else {
      Some(index(resentatativeFingerprint).fullFileFingerprint)
    }
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

  /**
   * query the chunk index.
   * If countIO is false, the IO is not recorded. This makes the simulation a bit easier.
   */
  def loadFullBin(resentatativeFingerprint: Digest, countIO: Boolean): Option[Bin] = {
    val result = if (!index.contains(resentatativeFingerprint)) {
      None
    } else {
      Some(index(resentatativeFingerprint))
    }

    val isCache = cache.contains(resentatativeFingerprint)
    if (isCache) {
      stats.cacheHitCount += 1
      result
    } else {
      if (countIO) {
        val indexPage = getPageForDigest(resentatativeFingerprint)
        logger.debug("Load bin %s, %s".format(resentatativeFingerprint, indexPage))
        result match {
          case None =>
            // We record a single page in the miss case
            // Note there should never be a miss case when getFullFileFingerprint has been called before
            stats.storageByteCount += 4 * 1024

            ioTrace.record(0, 2, indexPage, 4 * 1024, 1)
          case Some(bin) =>
            // The size depends on the chunks in the bin
            val ioSize = roundUpTo(bin.onDiskSize(), 4 * 1024)
            stats.storageByteCount += ioSize

            ioTrace.record(0, 2, indexPage, ioSize, 1)
        }

        stats.storageCount += 1
        stats.lookupCount += 1
      }
      stats.cacheMissCount += 1
      cache.update(resentatativeFingerprint, 1)
    }

    result
  }
}

class ExtremeBinningConfig(val dataSet: String,
  val blockSize: Int,
  val cacheSize: Int,
  val useBlockFingerprints: Boolean,
  val countChunkIndex: Boolean, /* switch between exact and non-exact version */
  val chunkIndexPageSize: Int,
  val negativeLookupProbabiliy: Double) {

}

class ExtremeBinningStatistics(var fileNumber: Int) {
  var fileCount: Int = 0
  var chunkCount: Int = 0
  var fullFileHit: Int = 0
  var fullFileHitChunkCount: Int = 0
  var chunkMissCount: Int = 0
  var chunkHitCount: Int = 0

  var repDigestHitCount: Int = 0
  var repDigestMissCount: Int = 0

  var binIndexStats: BinIndexStatistics = null
  var chunkIndexStats: ChunkIndexStatistics = null

  var uniqueChunkCount: Int = 0

  var totalStorageCount: Long = 0
  var totalStorageByteCount: Long = 0

  var duplicatedStoredChunkCount = 0

  def finish() {
    binIndexStats.finish()
    chunkIndexStats.finish()

    totalStorageCount = binIndexStats.storageCount + chunkIndexStats.storageCount
    totalStorageByteCount = binIndexStats.storageByteCount + chunkIndexStats.storageByteCount
  }
}

/**
 * The extreme binning handler simulates the extreme binning approach.
 */
class ExtremeBinningHandler(config: ExtremeBinningConfig, output: Option[String], ioTrace: DiskSimTrace) extends DefaultFileDataHandler with Log {

  /**
   * I maintain a chunk index just for comparision. It is not counted against extreme binning
   */
  val chunkIndex = new ChunkIndex(16 * 1024 * 1024, config.chunkIndexPageSize, config.negativeLookupProbabiliy, ioTrace)

  var currentStats: ExtremeBinningStatistics = null;
  val stats = new ListBuffer[ExtremeBinningStatistics]
  val gson = new GsonBuilder().setPrettyPrinting().create()
  val binIndex = new BinIndex(16 * 1024 * 1024, config, ioTrace)
  val digestFactory = new DigestFactory("MD5", 8)

  var traceFileCount = 0
  var currentBlockId = 0
  var blockIdAtStart = 0

  val currentBlockMapping = ListBuffer[Digest]()

  logger.info(gson.toJson(config))

  override def beginFile(traceFileNumber: Int) {
    logger.info("Begin file %s (%s)".format(traceFileNumber, traceFileCount))
    traceFileCount += 1

    currentStats = new ExtremeBinningStatistics(traceFileNumber)

    blockIdAtStart = currentBlockId
    logger.debug("Block id at start: %s".format(blockIdAtStart))
  }

  override def endFile(traceFileNumber: Int) {
    logger.info("End file %s".format(traceFileNumber))

    if (currentBlockMapping.length > 0) {
      finishBlock(currentBlockMapping.toList)
      currentBlockId += 1
      currentBlockMapping.clear()
    }

    currentStats.binIndexStats = binIndex.finish()
    currentStats.chunkIndexStats = chunkIndex.stats

    currentStats.finish()
    logger.info(gson.toJson(currentStats))
    stats.append(currentStats)

    chunkIndex.finish()

    currentStats = null
    currentBlockId += 1
  }

  /**
   * gets the representative fingerprint of a chunk list.
   * In the extreme binning approach, they use the smallest fingerprint.
   */
  def getRepresentativeFingerprint(chunkList: Seq[Digest]): Digest = {
    val comparator = UnsignedBytes.lexicographicalComparator()

    def lessThenDigest(d1: Digest, d2: Digest): Boolean = {
      comparator.compare(d1.digest, d2.digest) < 0
    }

    var minimalChunkDigest = chunkList.head
    for (chunk <- chunkList.tail) {
      if (lessThenDigest(chunk, minimalChunkDigest)) {
        minimalChunkDigest = chunk
      }
    }
    minimalChunkDigest
  }

  /**
   * calculates the full file fingerprint from a chunk list
   */
  def getFullFileFingerprint(chunkList: Seq[Digest]): Digest = {
    val builder = digestFactory.builder()
    for (chunk <- chunkList) {
      builder.append(chunk.digest, 0, chunk.digest.length)
    }
    builder.build()
  }

  def finishBlock(block: List[Digest]) {
    val representativeChunk = getRepresentativeFingerprint(block)
    val fullFileDigest = getFullFileFingerprint(block)

    logger.debug("rep chunk id %s, full file %s".format(representativeChunk, fullFileDigest))

    binIndex.getFullFileFingerprint(representativeChunk) match {
      case None =>
        // the fingerprint is not known. There is not much we can do
        logger.debug("rep chunk id %s not found".format(representativeChunk))
        // representative fingerprint not found
        currentStats.repDigestMissCount += 1
        val chunkSet = Set.empty[Digest]
        for (chunk <- block) {
          chunkSet += chunk
          currentStats.chunkMissCount += 1

          // just for comparision and statistics; not counted
          chunkIndex.apply(chunk, config.countChunkIndex) match {
            case None =>
              val cm = new ChunkMapping(0)
              chunkIndex.add(chunk, cm)
              currentStats.uniqueChunkCount += 1
            case Some(cm) =>
              currentStats.duplicatedStoredChunkCount += 1
          }
        }
        binIndex.add(representativeChunk, fullFileDigest, chunkSet)

      case Some(storedFullFileFingerprint) =>
        logger.debug("rep chunk id %s found".format(representativeChunk))

        currentStats.repDigestHitCount += 1

        if (config.useBlockFingerprints && storedFullFileFingerprint == fullFileDigest) {
          logger.debug("full block fingerprint hit".format(representativeChunk))
          // full file fingerprint matches
          currentStats.fullFileHit += 1
          currentStats.fullFileHitChunkCount += block.size
        } else {
          val bin = binIndex.loadFullBin(representativeChunk, true) match {
            case None => throw new Exception("Should not happen")
            case Some(b) => b
          }

          for (chunk <- block) {
            if (!bin.chunks.contains(chunk)) {
              // Update bin if new fp occurs
              logger.debug("Add chunk %s to bin".format(chunk))
              bin.chunks += chunk
              currentStats.chunkMissCount += 1

              // just for comparision and statistics; not counted
              chunkIndex.apply(chunk, config.countChunkIndex) match {
                case None =>
                  val cm = new ChunkMapping(0)
                  chunkIndex.add(chunk, cm)
                  currentStats.uniqueChunkCount += 1
                case Some(cm) =>
                  currentStats.duplicatedStoredChunkCount += 1

              }

            } else {
              currentStats.chunkHitCount += 1
            }

            // we do not update the full file fingerprint
          }
        }
    }
  }

  def handleFullFile(f: File, chunkList: Seq[Chunk]) {
    logger.debug("Handle file %s".format(f.filename))

    currentStats.fileCount += 1
    currentStats.chunkCount += chunkList.size

    if (chunkList.size == 0) {
      //handle degenerated case of an empty file
      return
    }

    var offset: Int = 0
    for (chunk <- chunkList) {
      // update the block id
      if ((offset + chunk.size) >= config.blockSize) {
        // Finish block
        finishBlock(currentBlockMapping.toList)
        currentBlockId += 1
        currentBlockMapping.clear()

        offset = (offset + chunk.size) % config.blockSize
      } else {
        offset += chunk.size
      }
      currentBlockMapping += chunk.fp
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
