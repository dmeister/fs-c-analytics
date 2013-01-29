/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import java.nio.charset.Charset
import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.{ Set => MutableSet }
import com.google.common.io.Files
import com.google.common.primitives.UnsignedBytes
import com.google.gson.GsonBuilder
import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.chunker.DigestFactory
import de.pc2.dedup.chunker.File
import de.pc2.dedup.util.Log
import de.pc2.dedup.DefaultFileDataHandler
import scala.math.pow
import java.math.BigInteger
import com.google.common.hash.Hashing

/**
 * Model of a sparse indexing segment
 */
case class Segment(val chunks: Set[Digest], val offset: Long) {

  def onDiskSize = 24 + (size * 20)

  def size = chunks.size
}

/**
 * Sparse index statistics
 */
class SparseIndexStatistics() {
  var lookupCount: Int = 0
  var updateCount: Int = 0
  var storageCount: Long = 0
  var storageByteCount: Long = 0

  var cacheHitCount: Int = 0
  var cacheMissCount: Int = 0

  def finish() {
  }
}

/**
 * Sparse index model for the simulation
 */
class SparseIndex(indexPageCount: Int, config: SparseIndexingConfig, ioTrace: DiskSimTrace) extends Log {
  /** Mapping from the representative fingerprint to the bin */
  val index = Map.empty[Digest, ListBuffer[Segment]]
  var currentSegmentOffset : Long = 0

  val cache = new LRU[Segment, Int](config.cacheSize, segmentCacheEvict)

  var stats = new SparseIndexStatistics()

  def segmentCacheEvict(segment: Segment, usedCount: Int): Boolean = {
    logger.debug("Evict segment %s".format(segment))
    true
  }

  def finish(): SparseIndexStatistics = {

    // get average bin size

    val previousStats = stats

    stats = new SparseIndexStatistics()
    previousStats
  }

  def getPageForDigest(fp: Digest): Int = {
    return scala.math.abs(fp.hashCode) % indexPageCount
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

  def addSegment(chunks: Seq[Digest]) = {
    val segment = new Segment(chunks.toSet, currentSegmentOffset)
    currentSegmentOffset += roundUpTo(segment.onDiskSize, 4 * 1024)

    //logger.debug("Add segment: size %s, offset %s".format(chunks.length, segment.offset))

    for (chunk <- getHooks(segment.chunks.toSeq)) {
      val buffer = index.get(chunk) match {
        case Some(buffer) =>

          buffer += segment
          if (buffer.length > config.maxHooksPerSegment) {
            buffer.remove(0) // remove oldest
          }
          buffer
        case None =>
          val buffer = new ListBuffer[Segment]
          buffer += segment
          index.update(chunk, buffer)
          buffer
      }
      //logger.debug("Add segment hook: %s: buffer size %s".format(chunk, buffer.size))
    }

    stats.updateCount += 1
  }

  /**
   * gets the hooks
   */
  def getHooks(chunkList: Seq[Digest]): Set[Digest] = {

    val hooks = MutableSet.empty[Digest]
    val mask = (~(0xFF << config.sampleFactor))

    for (chunk: Digest <- chunkList.toSet) {
      val isHook = (chunk.digest(0).toByte & mask.toByte) == 0

      if (isHook) {
        hooks += chunk
      }
    }
    if (hooks.isEmpty && chunkList.toSet.size <= config.zeroHookHandlingLimit)
      chunkList.toSet
    else
      hooks.toSet
  }

  /**
   * query the chunk index.
   * If countIO is false, the IO is not recorded. This makes the simulation a bit easier.
   */
  def getSegments(hooks: Set[Digest], maxSegmentCount: Int, countIO: Boolean): List[Segment] = {
    //logger.debug("Get champion segments: incoming segment hooks %s".format(hooks))

    val champs = Map.empty[Segment, ListBuffer[Digest]]

    for (hook <- hooks) {
      index.get(hook) match {
        case Some(segments) =>
          for (segment <- segments) {
            champs.get(segment) match {
              case Some(buffer) => buffer += hook
              case None =>
                val buffer = new ListBuffer[Digest]
                buffer += hook
                champs.update(segment, buffer)
            }
          }
        case None =>
      }
    }
    //logger.debug("Found %s possible segments".format(champs.size))

    val champList = new ListBuffer[Segment]

    while (champList.length < maxSegmentCount && !champs.isEmpty) {
      val nextChampion = champs.toList.max(Ordering[Int].on[(Segment, ListBuffer[Digest])](_._2.length))
      //logger.debug("Selected champion: size %s, hook count %s".format(nextChampion._1.chunks.size, nextChampion._2))

      champs -= nextChampion._1

      for (matchedHook <- nextChampion._2) {
        // remove hooks
        index.get(matchedHook) match {
          case Some(segments) =>
            for (segment <- segments) {
              champs.get(segment) match {
                case Some(buffer) =>
                  buffer -= matchedHook
                  if (buffer.isEmpty) {
                    champs -= segment
                  }
                case None =>
              }
            }
          case None =>
        }
      }

      champList += nextChampion._1
    }

    //logger.debug("Found championlist: size %s".format(champList.length))

    // I now have the champ list
    if (countIO) {
      for (champion <- champList) {
        val isCache = cache.contains(champion)
        if (isCache) {
          stats.cacheHitCount += 1
        } else {
         logger.debug("Load champion: offset %s".format(champion.offset))
          val ioSize = roundUpTo(champion.onDiskSize, 4 * 1024)
          stats.storageCount += 1
          stats.storageByteCount += ioSize
          stats.cacheMissCount += 1

          ioTrace.record(0, 2, (champion.offset / 4 * 1024).toInt, ioSize, 1)
          
          cache.update(champion, 1)
        }
      }
    }
    for (champion <- champList) {
      stats.lookupCount += 1
    }

    champList.toList
  }
}

class SparseIndexingConfig(val dataSet: String,
  val segmentSize: Int,
  val fixedSegmentation: Boolean,
  val cacheSize: Int,
  val sampleFactor: Int,
  val maxSegmentCount: Int,
  val maxHooksPerSegment: Int,
  val endSegmentPerFile: Boolean,
  val zeroHookHandlingLimit: Int,
  val countChunkIndex: Boolean,
  val chunkIndexPageSize: Int,
  val negativeLookupProbabiliy: Double,
  val useCache:Boolean) {

}

class SparseIndexingStatistics(var fileNumber: Int) {
  var fileCount: Int = 0
  var chunkCount: Int = 0
  var chunkMissCount: Int = 0
  var chunkMissCountWithSegments: Int = 0
  var chunkHitCount: Int = 0

  var sparseIndexStats: SparseIndexStatistics = null
  var chunkIndexStats: ChunkIndexStatistics = null

  var uniqueChunkCount: Int = 0

  var totalStorageCount: Long = 0
  var totalStorageByteCount: Long = 0

  var duplicatedStoredChunkCount = 0

  var zeroHookCount = 0
  var zeroHookItemCount = 0

  def finish() {
    sparseIndexStats.finish()
    chunkIndexStats.finish()

    totalStorageCount = sparseIndexStats.storageCount + chunkIndexStats.storageCount
    totalStorageByteCount = sparseIndexStats.storageByteCount + chunkIndexStats.storageByteCount
  }
}

/**
 * The sparse indexing handler simulates the sparse indexing approach.
 */
class SparseIndexingHandler(config: SparseIndexingConfig, output: Option[String], ioTrace: DiskSimTrace) extends DefaultFileDataHandler with Log {

  /**
   * I maintain a chunk index just for comparision. It is not counted against sparse indexing
   */
  val chunkIndex = new ChunkIndex(16 * 1024 * 1024, config.chunkIndexPageSize, config.negativeLookupProbabiliy, ioTrace)

  var currentStats: SparseIndexingStatistics = null;
  val stats = new ListBuffer[SparseIndexingStatistics]
  val gson = new GsonBuilder().setPrettyPrinting().create()
  val sparseIndex = new SparseIndex(16 * 1024 * 1024, config, ioTrace)

  var traceFileCount = 0
  val currentSegment = ListBuffer[Digest]()
  var currentSegmentSize = 0

  logger.info(gson.toJson(config))

  override def beginFile(traceFileNumber: Int) {
    logger.info("Begin file %s (%s)".format(traceFileNumber, traceFileCount))
    traceFileCount += 1

    currentStats = new SparseIndexingStatistics(traceFileNumber)
  }

  override def endFile(traceFileNumber: Int) {
    logger.info("End file %s".format(traceFileNumber))
    finishSegment()

    currentStats.sparseIndexStats = sparseIndex.finish()
    currentStats.chunkIndexStats = chunkIndex.stats

    currentStats.finish()
    logger.info(gson.toJson(currentStats))
    stats.append(currentStats)

    chunkIndex.finish()

    currentStats = null
  }

  val segmentBreakmark: Int = (pow(2.0, BigInteger.valueOf(config.segmentSize / (8 * 1024)).bitLength() - 1) - 1).toInt
  val segmentHasher = Hashing.murmur3_32()

  def isSegmentLandmark(d: Digest): Boolean = {
    if (config.fixedSegmentation)
      false
    else {
      (segmentHasher.hashBytes(d.digest).asInt() & segmentBreakmark) == segmentBreakmark
    }
  }

  def isFound(segments: Seq[Segment], chunk: Digest): Boolean = {
    if (segments.isEmpty) false
    else if (segments.head.chunks.contains(chunk)) true
    else isFound(segments.tail, chunk)
  }

  def finishSegment() {
    //logger.debug("Finish segment: size %s".format(currentSegmentSize))

    if (currentSegment.isEmpty) {
      return
    }

    val hooks = sparseIndex.getHooks(currentSegment)
    if (hooks.isEmpty) {
      logger.debug("Failed to find hooks: current segment %s".format(currentSegment))
      currentStats.zeroHookCount += 1
      currentStats.zeroHookItemCount += currentSegment.size
    }
    val segments = sparseIndex.getSegments(hooks, config.maxSegmentCount, true)

    //logger.debug("Check segments: champion size %s".format(segments.size))

    var segmentMissCount = 0
    var segmentHitCount = 0
    var segmentFalseMissCount = 0

    for (chunk <- currentSegment) {
      if (isFound(segments, chunk)) {
        segmentHitCount += 1
        currentStats.chunkHitCount += 1
      } else {
        if (segments.length > 0) {
          currentStats.chunkMissCountWithSegments += 1
        }
        segmentMissCount += 1
        currentStats.chunkMissCount += 1

        // just for comparision and statistics; not counted
        chunkIndex.apply(chunk, config.countChunkIndex) match {
          case None =>
            val cm = new ChunkMapping(0)
            chunkIndex.add(chunk, cm)
            currentStats.uniqueChunkCount += 1
          case Some(cm) =>
            currentStats.duplicatedStoredChunkCount += 1
            segmentFalseMissCount += 1

        }
      }
    }
    logger.debug("Finished segment: champion size %s, segment size %s, hook count %s, hit %s, miss %s, false miss %s".format(segments.size,
      currentSegment.size, hooks.size,
      segmentHitCount, segmentMissCount, segmentFalseMissCount))
    sparseIndex.addSegment(currentSegment)

    currentSegment.clear()
    currentSegmentSize = 0
  }

  def handleFullFile(f: File, chunkList: Seq[Chunk]) {
    currentStats.fileCount += 1

    if (chunkList.size == 0) {
      //handle degenerated case of an empty file
      return
    }

    for (chunk <- chunkList) {
      currentStats.chunkCount += 1

      val add = if (config.fixedSegmentation) {
        if ((currentSegmentSize + chunk.size) >= config.segmentSize) {
          // Finish block
          finishSegment()
        }
        true
      } else {
        if (isSegmentLandmark(chunk.fp)) {
          currentSegment += chunk.fp
          currentSegmentSize += chunk.size

          finishSegment()
          false
        } else {
          true
        }
      }
      if (add) {
        currentSegment += chunk.fp
        currentSegmentSize += chunk.size
      }
    }

    if (config.endSegmentPerFile) {
      finishSegment()
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
