/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import java.nio.charset.Charset
import java.nio.ByteBuffer

import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

import com.google.common.io.Files
import com.google.gson.GsonBuilder

import de.pc2.dedup.chunker.rabin.RabinChunker
import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.chunker.DigestFactory
import de.pc2.dedup.chunker.File
import de.pc2.dedup.util.Log
import de.pc2.dedup.DefaultFileDataHandler

class ZeroOneCompressionConfig(val dataSet: String,
  val blockSize: Int) {
}

class ZeroOneCompressionStatistics(
  val fileNumber: Int) {

  var chunkCount: Long = 0
  var uniqueChunkCount: Long = 0
  var redundantChunkCount: Long = 0

  var blockIndexStats: SizeCountingBlockIndexStatistics = null
  var chunkIndexStats: ChunkIndexStatistics = null
  var codeWordStats: ZeroOneCodeWordIndexStatistics = null

  def finish() {
    blockIndexStats.finish()
    chunkIndexStats.finish()
    codeWordStats.finish()
  }
}

class ZeroOneCodeWordIndexStatistics() {
  var zeroHits: Long = 0
  var oneHits: Long = 0
  var misses: Long = 0

  def finish() {
  }
}

object CodeWordDigest {
  def fromLong(l: Long, codeWordLength: Int) : Digest = {
    val bytes = ByteBuffer.allocate(8).putLong(l).array()
    new CodeWordDigest(bytes, codeWordLength)
  }
}

class CodeWordDigest(digest: Array[Byte], val codeWordLength: Int) extends Digest(digest) {

}

class ZeroOneCodeWordIndex(val zeroCodeWordLength: Int) extends Log {
  val zeroIndex = Map.empty[Digest, Digest]
  val oneIndex = Map.empty[Digest, Digest]
  val zeroCodeWord = new CodeWordDigest(ByteBuffer.allocate(1).put(1.toByte).array(), zeroCodeWordLength)
  val oneCodeWord = new CodeWordDigest(ByteBuffer.allocate(1).put(2.toByte).array(), zeroCodeWordLength)

  var stats = new ZeroOneCodeWordIndexStatistics()

  def finish() {
    stats = new ZeroOneCodeWordIndexStatistics()
  }

  def getFullChunkDigest(v: Byte, size: Int, digestSize: Int): Digest = {
    val bb = ByteBuffer.allocate(size)
    while (bb.remaining > 0) {
      bb.put(v)
    }
    bb.rewind()
    var chunk: Chunk = null
    val rc = new RabinChunker(2 * 1024, 8 * 1024, 32 * 1024, false, new DigestFactory("SHA-1", digestSize, None), "c8")
    val rcs = rc.createSession()
    rcs.chunk(bb) { c: Chunk =>
      if (chunk == null) {
        chunk = c
      }
    }
    if (chunk == null) {
      rcs.close() { c: Chunk =>
        if (chunk == null) {
          chunk = c
        }
      }
    }
    logger.debug("Special chunk %s".format(chunk.fp))
    
    chunk.fp
  }
  
  def fillCodeWordMap(map: Map[Digest, Digest], v: Byte, size:Int, codeWord: Digest) {
    for (i <- 8 to 20)  {
      map += getFullChunkDigest(v, size, i) -> codeWord
    }
  }

  fillCodeWordMap(zeroIndex, 0, 8 * 1024, zeroCodeWord)
  fillCodeWordMap(zeroIndex, 0, 16 * 1024, zeroCodeWord)
  fillCodeWordMap(zeroIndex, 0, 32 * 1024, zeroCodeWord)
  fillCodeWordMap(oneIndex, 255.toByte, 32 * 1024, oneCodeWord)

  def lookupDigest(fp: Digest): Option[Digest] = {
    if (zeroIndex.contains(fp)) {
      stats.zeroHits += 1
      Some(zeroCodeWord)
    } else if (zeroIndex.contains(fp)) {
      stats.oneHits += 1
      Some(oneCodeWord)
    } else {
      stats.misses += 1
      None
    }
  }
}

