/*
 * fsc-analytics
 */
package de.pc2.dedup

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.chunker.File
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.fschunk.handler.FileDataHandler

/**
 * Default class for all other file handlers.
 * The main purpose is the collect the file parts and the matching files and deliver them togehter.
 */
trait DefaultFileDataHandler extends FileDataHandler {
  val filePartialMap = Map.empty[String, ListBuffer[Chunk]]
 
  def beginFile(traceFileNumber: Int) {
  }

  def endFile(traceFileNumber: Int) {
  }
  
  def handle(fp: FilePart) {
      if (!filePartialMap.contains(fp.filename)) {
        filePartialMap += (fp.filename -> new ListBuffer[Chunk]())
      }
      for (chunk <- fp.chunks) {
        filePartialMap(fp.filename).append(chunk)
      }
  }
  
  private def getAllFileChunks(f: File) : scala.collection.Seq[Chunk] = {
      if (filePartialMap.contains(f.filename)) {
          val partialChunks = filePartialMap(f.filename)
          filePartialMap -= f.filename
          List.concat(partialChunks, f.chunks)
      } else {
          f.chunks
      }
  }

  def handle(f: File) {
    handleFullFile(f, getAllFileChunks(f))
  }
  
  def handleFullFile(f: File, chunkList : scala.collection.Seq[Chunk])
}
