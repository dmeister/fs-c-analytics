/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import java.io.BufferedWriter
import java.io.FileWriter

/**
 * Trait to allow the generation of disksim trace files
 */
trait DiskSimTrace {
  def record(arrivalTime: Double, deviceNumber: Int, blockNumber: Int, requestSize: Int, requestTag: Int)
}

/**
 * ASCII disk sim trace format
 */
class ASCIIDiskSimTrace(filename: String) extends DiskSimTrace {
  val writer = new BufferedWriter(new FileWriter(filename))

  def record(arrivalTime: Double, deviceNumber: Int, blockNumber: Int, requestSize: Int, requestTag: Int) {
    writer.write("%s\t%s\t%s\t%s\t%s\n".format(arrivalTime, deviceNumber, blockNumber, requestSize, requestTag))
  }
}

/**
 * Dummy implementation of the DiskSimTrace trait
 */
class NoDiskSimTrace extends DiskSimTrace {
  def record(arrivalTime: Double, deviceNumber: Int, blockNumber: Int, requestSize: Int, requestTag: Int) {
  }
}