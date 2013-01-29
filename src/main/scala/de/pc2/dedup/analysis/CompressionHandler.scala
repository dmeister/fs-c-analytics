/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

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

class CompressionConfig(
  val dataSet: String,
  val blockSize: Int,
  val fullDigestLength: Int,
  val pageDigestLength: Int,
  val useDict: Boolean,
  val dictCodeWordLength: Int,
  val dictThreshold: Double,
  val useHigherOrderDict: Boolean,
  val higherOrderCodeWordLength: Int,
  val misraGriedLength: Int,
  val useZeroHandling: Boolean,
  val zeroCodeWordLength: Int,
  val revoke: Boolean) {
}

class CompressionStatistics(
  val fileNumber: Int) {

  var chunkCount: Long = 0
  var uniqueChunkCount: Long = 0
  var redundantChunkCount: Long = 0

  var assignedHigherOrderCodeWordCount: Long = 0
  var changedHigherOrderCodeWordRequest: Long = 0
  var revokeDictCodewordRequest: Long = 0

  var usedZeroDigestCount: Long = 0
  var usedHigherOrderDigestCount: Long = 0
  var usedDictDigestCount: Long = 0
  var usedPageDigestCount: Long = 0

  var blockIndexStats: SizeCountingBlockIndexStatistics = null
  var chunkIndexStats: ChunkIndexStatistics = null
  var codeWordStats: CodeWordIndexStatistics = null
  var zeroChunkStats: ZeroOneCodeWordIndexStatistics = null

  def finish() {
    blockIndexStats.finish()
    chunkIndexStats.finish()
    codeWordStats.finish()
    zeroChunkStats.finish()
  }
}

class CodeWordIndexStatistics() {
  var assignedCodeWords: Long = 0
  var codeWordCount: Long = 0

  def finish() {
  }
}

class CodeWordIndex(val codeWordLength: Int) {
  val index = Map.empty[Digest, Digest]
  val reverseIndex = Map.empty[Digest, Digest]

  var nextFreeCodeWord: Long = 1L
  var stats = new CodeWordIndexStatistics()

  def finish() {
    stats = new CodeWordIndexStatistics()
  }

  def long2digest(l: Long): Digest = {
    CodeWordDigest.fromLong(l, codeWordLength)
  }

  def contains(fp: Digest): Boolean = {
    return index.contains(fp)
  }

  def lookupDigest(fp: Digest): Option[Digest] = {
    if (index.contains(fp)) {
      Some(index(fp))
    } else {
      None
    }
  }

  def assignCodeWord(fp: Digest): Digest = {
    val codeWord = long2digest(nextFreeCodeWord)
    nextFreeCodeWord += 1

    stats.assignedCodeWords += 1
    index += (fp -> codeWord)
    reverseIndex += (codeWord -> fp)

    return codeWord
  }
  
  def deactivateCodeWord(fp: Digest)  = {
     val cw = index(fp)
     index -= fp
     reverseIndex -= cw
  }

}

// combines multiple compression variants
class CompressionHandler(config: CompressionConfig, output: Option[String])
  extends DefaultFileDataHandler with Log {

  val blockIndex = new SizeCountingBlockIndex(config.fullDigestLength)
  val chunkIndex = new ChunkIndex(16 * 1024 * 1024, 4 * 1024, 1.0, new NoDiskSimTrace())
  val codeWordIndex = new CodeWordIndex(config.dictCodeWordLength)
  var zeroHandling = new ZeroOneCodeWordIndex(config.zeroCodeWordLength)

  val orderStatisticsMap: Map[Digest, FrequencyEstimator[Digest]] = Map.empty[Digest, FrequencyEstimator[Digest]]
  val higherOrderCodeWord = CodeWordDigest.fromLong(1, config.higherOrderCodeWordLength)

  var currentBlockId = 0
  var traceFileCount = 0
  var blockIdAtStart = 0

  var currentStats: CompressionStatistics = null;
  val stats = new ListBuffer[CompressionStatistics]
  val gson = new GsonBuilder().setPrettyPrinting().create()

  override def beginFile(traceFileNumber: Int) {
    logger.info("Begin file %s (%s)".format(traceFileNumber, traceFileCount))
    currentStats = new CompressionStatistics(traceFileNumber)

    blockIdAtStart = currentBlockId
    logger.debug("Block id at start: %s".format(blockIdAtStart))
  }

  def assignCodeWords() {

    // order-0 code word assignment
    if (config.useDict) {
      var totalUsageCount: Long = 0
      for ((d, cm) <- chunkIndex.index) {
        totalUsageCount += cm.usageCount
      }
      val log2 = scala.math.log(2)

      val entropyThreshold = (scala.math.log(chunkIndex.index.size) / log2) * config.dictThreshold
      for ((d, cm) <- chunkIndex.index) {
        val p = 1.0 * cm.usageCount / totalUsageCount
        val entropy = -1.0 * (scala.math.log(p) / log2)
        logger.debug("%s, uc %s, p %f, e %f".format(d, cm.usageCount, p, entropy))

        if (entropy <= entropyThreshold && !codeWordIndex.contains(d)) {
          val cw = codeWordIndex.assignCodeWord(d)
          logger.debug("%s: Assigned code word %s".format(d, cw))
        } else if (config.revoke && entropy > entropyThreshold && codeWordIndex.contains(d)) {
          currentStats.revokeDictCodewordRequest += 1
          codeWordIndex.deactivateCodeWord(d)
          logger.debug("%s: Revoked code word".format(d))
        }
      }
    }
    // higher-order code word assignment

    if (config.useHigherOrderDict) {
      assignOrder1CodeWords()
    }
  }

  override def endFile(traceFileNumber: Int) {
    logger.info("End file %s".format(traceFileNumber))

    if (config.useDict || config.useHigherOrderDict) {
      assignCodeWords()
    }

    currentBlockId += 1

    codeWordIndex.stats.codeWordCount = codeWordIndex.index.size
    chunkIndex.stats.chunkCount = chunkIndex.index.size

    currentStats.blockIndexStats = blockIndex.stats
    currentStats.chunkIndexStats = chunkIndex.stats
    currentStats.codeWordStats = codeWordIndex.stats
    currentStats.zeroChunkStats = zeroHandling.stats
    currentStats.finish()

    logger.info(gson.toJson(currentStats))
    stats.append(currentStats)
    currentStats = null

    blockIndex.finish()
    chunkIndex.finish()
    codeWordIndex.finish()

    traceFileCount += 1
  }

  def updateBlock(blockId: Int, itemList: ListBuffer[Digest]) {
    // get a new copy
    val bm = new BlockMappingList(itemList.toList)
    blockIndex.add(blockId, bm)
  }

  def handleFullFile(f: File, chunkList: Seq[Chunk]) {
    logger.debug("Handle file %s".format(f.filename))

    var offset: Int = 0
    val currentBlockMapping = ListBuffer[Digest]()
    var lastDigest: Digest = null

    var predictedDigest: Option[Digest] = None
    var lastNewDigest: Option[Digest] = None

    for (chunk <- chunkList) {
      currentStats.chunkCount += 1

      // update the block id
      if ((offset + chunk.size) >= config.blockSize) {
        // Finish block
        //currentBlockMapping += chunk.fp
        updateBlock(currentBlockId, currentBlockMapping)
        currentBlockId += 1
        currentBlockMapping.clear()
        predictedDigest = None

        offset = (offset + chunk.size) % config.blockSize
      } else {
        offset += chunk.size
      }

      if (!lastNewDigest.isEmpty && config.misraGriedLength == -1) {
        // first wins prediction
        // the first next chunk wins
        chunkIndex(lastNewDigest.get, false).get.topNextChunk = Some(chunk.fp)
      }

      // check chunk index first
      val digestToUse = chunkIndex(chunk.fp, false) match {
        case None =>
          currentStats.uniqueChunkCount += 1

          val cm = new ChunkMapping()
          cm.markBlock(currentBlockId)
          chunkIndex.add(chunk.fp, cm)

          var digestToUseOption: Option[Digest] = if (config.useZeroHandling) {
            zeroHandling.lookupDigest(chunk.fp) match {
              case None => None
              case Some(d) =>
                currentStats.usedZeroDigestCount += 1
                Some(d)
            }
          } else {
            None
          }

          lastNewDigest = Some(chunk.fp)

          if (digestToUseOption.isEmpty) {
            if (config.pageDigestLength < config.fullDigestLength) {
              currentStats.usedPageDigestCount += 1
            }
            new CodeWordDigest(chunk.fp.digest, config.pageDigestLength)
          } else {
            digestToUseOption.get
          }
        case Some(cm) =>
          currentStats.redundantChunkCount += 1
          cm.markBlock(currentBlockId)

          logger.debug("Found chunk %s: block id %s".format(
            chunk.fp, currentBlockId))

          lastNewDigest = None

          var digestOption: Option[Digest] = if (config.useZeroHandling) {
            zeroHandling.lookupDigest(chunk.fp) match {
              case None => None
              case Some(d) =>
                currentStats.usedZeroDigestCount += 1
                Some(d)
            }
          } else {
            None
          }

          digestOption = digestOption match {
            case Some(d) => Some(d)
            case None => if (config.useHigherOrderDict && traceFileCount > 0) {
              if (!predictedDigest.isEmpty && predictedDigest.get == chunk.fp) {
                currentStats.usedHigherOrderDigestCount += 1
                Some(higherOrderCodeWord)
              } else {
                None
              }
            } else {
              None
            }
          }
          digestOption = digestOption match {
            case Some(d) => Some(d)
            case None => if (config.useDict) {
              val cw = codeWordIndex.lookupDigest(chunk.fp)
              if (!cw.isEmpty) {
                currentStats.usedDictDigestCount += 1
              }
              cw
            } else {
              None
            }
          }

          digestOption = digestOption match {
            case Some(d) => Some(d)
            case None =>
              if (config.pageDigestLength < config.fullDigestLength) {
                currentStats.usedPageDigestCount += 1
              }
              Some(new CodeWordDigest(chunk.fp.digest, config.pageDigestLength))
          }

          // update the predictions
          predictedDigest = cm.topNextChunk

          digestOption.get
      }
      currentBlockMapping += digestToUse

      updateOrderStatistics(lastDigest, chunk)
      lastDigest = chunk.fp
    }
    updateBlock(currentBlockId, currentBlockMapping)
    currentBlockId += 1
  }

  private def updateOrderStatistics(lastDigest: de.pc2.dedup.chunker.Digest, chunk: de.pc2.dedup.chunker.Chunk) {
    if (config.misraGriedLength < 0) {
      // special handling
      return
    }
    def createNewEstimator(): FrequencyEstimator[Digest] = {
      if (config.misraGriedLength > 0) {
        new MisraGries[Digest](config.misraGriedLength)
      } else {
        new MapFrequenceEstimator[Digest]()
      }
    }
    if (config.useHigherOrderDict) {
      if (!orderStatisticsMap.contains(lastDigest)) {
        orderStatisticsMap += (lastDigest -> createNewEstimator())
      }
      val mg = orderStatisticsMap(lastDigest)
      mg.add(chunk.fp)
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

  private def assignOrder1CodeWords(): Unit = {
    if (config.misraGriedLength < 0) {
      // special handling
      return
    }
    for ((digest, mg) <- orderStatisticsMap) {

      chunkIndex(digest, false) match {
        case None => // pass
        case Some(cm) =>
          if (cm.topNextChunk.isEmpty) {
            mg.getMaxValue() match {
              case None => // pass
              case Some(predictionDigest) =>
                currentStats.assignedHigherOrderCodeWordCount += 1
                cm.topNextChunk = Some(predictionDigest)
                logger.debug("%s/%s: Assigned code word".format(digest, predictionDigest))
            }
          } else {
            mg.getMaxValue() match {
              case None => // pass
              case Some(predictionDigest) =>
                if (!predictionDigest.equals(cm.topNextChunk.get) && config.revoke) {
                  currentStats.changedHigherOrderCodeWordRequest += 1
                  cm.topNextChunk = Some(predictionDigest)
                  logger.debug("%s/%s: Re-assigned code word".format(digest, predictionDigest))
                }
            }
          }
      }
    }
  }
}
