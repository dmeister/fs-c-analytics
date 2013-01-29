/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import java.io.File
import java.util.regex.Matcher
import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import de.pc2.dedup.fschunk.format.Format
import de.pc2.dedup.util.Log

/**
 * A object containing all available data sets.
 * TODO (dmeister) Avoid having them hard coded
 */
object DataSet {
  def apply(name: String) = new BaseDataSet(name)
}

/**
 * Abstract data set class
 */
abstract class DataSet(val directory: String) {
  def getFileNumber(file: File): Option[Int]
  def getFormat(): Format

  def runFileList(dataDirectory: String): List[(File, Int)] = {
    val format = getFormat()

    val path = new File(dataDirectory, directory)
    val files = new ListBuffer[(File, Int)]()

    val fileList = path.listFiles()
    if (fileList == null) {
      throw new Exception("Invalid data directory %s".format(path))
    }
    for (f <- fileList) {
      val fn = getFileNumber(f)
      fn match {
        case Some(fileNumber) => files.append((f, fileNumber))
        case None =>
      }
    }
    val sortedFiles = if (inverseSorting) { 
      files.sortBy { _._2 }.reverse
    } else {
        files.sortBy { _._2 }    
    }
    return sortedFiles.toList
  }
  
  val inverseSorting = false
}

/**
 * Data set class for data sets
 */
class BaseDataSet(pattern: String) extends DataSet(pattern) {
  val fileNumberPattern = Pattern.compile("week_(\\d+)_-cdc8$")
		  									
  def getFileNumber(file: File): Option[Int] = {
    val m: Matcher = fileNumberPattern.matcher(file.getName())
    if (!m.matches()) {
      return None
    }
    val s = m.group(1)
    return Some(Integer.valueOf(s))
  }

  def getFormat(): Format = {
    return Format("protobuf");
  }
  
    override val inverseSorting = true
}

