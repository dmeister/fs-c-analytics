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

class ChunkIndexConfig(
    val dataSet: String, 
    val blockSize: Int,
    val chunkIndexPageSize: Int) {
}

class ChunkIndexHandlerStatistics( 
    val fileNumber: Int) {

    var chunkCount: Long = 0
    var uniqueChunkCount: Long = 0
    var redundantChunkCount: Long = 0

    var blockIndexStats: BlockIndexStatistics = null
    var chunkIndexStats: ChunkIndexStatistics = null

    def finish() {
        blockIndexStats.finish()
        chunkIndexStats.finish()
    }
}

class ChunkIndexHandler(config : ChunkIndexConfig, output: Option[String], ioTrace: DiskSimTrace) 
  extends DefaultFileDataHandler with Log {

  val blockIndex = new BlockIndex(config.blockSize, 4 * 1024, ioTrace)
  val chunkIndex = new ChunkIndex(16 * 1024 * 1024, config.chunkIndexPageSize, 1.0, ioTrace)
  var currentBlockId = 0
  var traceFileCount = 0
  var blockIdAtStart = 0
  
  var currentStats : ChunkIndexHandlerStatistics = null;
  val stats = new ListBuffer[ChunkIndexHandlerStatistics]
  val gson = new GsonBuilder().setPrettyPrinting().create()
  
  override def beginFile(traceFileNumber: Int) {
      logger.info("Begin file %s (%s)".format(traceFileNumber, traceFileCount))
      traceFileCount += 1
      currentStats = new ChunkIndexHandlerStatistics(traceFileNumber)
      
      blockIdAtStart = currentBlockId
      logger.debug("Block id at start: %s".format(blockIdAtStart))
  }
  
  override def endFile(traceFileNumber: Int) {
      logger.info("End file %s".format(traceFileNumber))

      currentBlockId += 1

      chunkIndex.stats.chunkCount = chunkIndex.index.size

      currentStats.blockIndexStats = blockIndex.stats
      currentStats.chunkIndexStats = chunkIndex.stats
      currentStats.finish()   

      logger.info(gson.toJson(currentStats))
      stats.append(currentStats)
      currentStats = null

      blockIndex.finish()
      chunkIndex.finish()
  }
  
  def updateBlock(blockId: Int, itemSet: ListBuffer[Digest]) {
      // get a new copy
      val bms : Set[Digest] = Set(itemSet : _*)
      val bm = new BlockMapping(bms)
      blockIndex.add(blockId, bm)
  }

  def handleFullFile(f: File, chunkList : Seq[Chunk]) {
      logger.debug("Handle file %s".format(f.filename))
      
      var offset : Int = 0
      val currentBlockMapping = ListBuffer[Digest]()
      for (chunk <- chunkList) {
        currentStats.chunkCount += 1

        // update the block id
        if ((offset + chunk.size) >= config.blockSize) {
            // Finish block
            currentBlockMapping += chunk.fp
            updateBlock(currentBlockId, currentBlockMapping)
            currentBlockId += 1
            currentBlockMapping.clear()
                                
            offset = (offset + chunk.size) % config.blockSize
        } else {
            offset += chunk.size
        }        
        
        // check chunk index first
        chunkIndex(chunk.fp, false) match {
          case None =>
            currentStats.uniqueChunkCount += 1
            
            val cm = new ChunkMapping()
            cm.markBlock(currentBlockId)
            chunkIndex.add(chunk.fp, cm)

            currentBlockMapping += chunk.fp
          case Some(cm) =>
            currentStats.redundantChunkCount += 1
            cm.markBlock(currentBlockId)

            logger.debug("Found chunk %s: block id %s".format(
                chunk.fp, currentBlockId))

            // found before
            currentBlockMapping += chunk.fp
        }
      }
      
      updateBlock(currentBlockId, currentBlockMapping)
      currentBlockId += 1
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
