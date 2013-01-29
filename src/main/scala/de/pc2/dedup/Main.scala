/*
 * fsc-analytics
 */
package de.pc2.dedup

import java.io.FileReader
import org.apache.log4j.xml.DOMConfigurator
import org.clapper.argot.ArgotParser
import org.clapper.argot.ArgotConverters
import com.google.gson.Gson
import de.pc2.dedup.analysis.CompressionConfig
import de.pc2.dedup.analysis.ContainerCacheConfig
import de.pc2.dedup.analysis.ExtremeBinningConfig
import de.pc2.dedup.analysis.ASCIIDiskSimTrace
import de.pc2.dedup.analysis.BlockIndexFilterConfig
import de.pc2.dedup.analysis.BlockIndexFilterHandler
import de.pc2.dedup.analysis.CompressionHandler
import de.pc2.dedup.analysis.ContainerCacheHandler
import de.pc2.dedup.analysis.DataSet
import de.pc2.dedup.analysis.ExtremeBinningHandler
import de.pc2.dedup.analysis.NoDiskSimTrace
import de.pc2.dedup.fschunk.Reporter
import de.pc2.dedup.util.SystemExitException
import de.pc2.dedup.fschunk.GCReporting
import de.pc2.dedup.analysis.ChunkIndexConfig
import de.pc2.dedup.analysis.ChunkIndexHandler
import de.pc2.dedup.analysis.SparseIndexingConfig
import de.pc2.dedup.analysis.SparseIndexingHandler
import de.pc2.dedup.compression.EntropyHandlerConfig
import de.pc2.dedup.compression.EntropyHandler

class Options(val dataSetDirectory: String, val reportInterval: Option[Int]) {
}

/**
 * Main object for the parser.
 * The parser is used to replay trace of chunking runs
 */
object Main {
  def handleBlockIndexFilterType(options: Options,
    configFilename: String,
    outputFilename: Option[String],
    ioTraceFilename: Option[String]) {
    val gson = new Gson()

    val config = gson.fromJson(
        new FileReader(configFilename), classOf[BlockIndexFilterConfig])

    val ds = DataSet(config.dataSet)
    val fileList = ds.runFileList(options.dataSetDirectory)
    val ioTrace = ioTraceFilename match {
      case Some(filename) => new ASCIIDiskSimTrace(filename)
      case None => new NoDiskSimTrace()
    }
    val handler = new BlockIndexFilterHandler(config, outputFilename, ioTrace)

    for ((f, fn) <- fileList) {

      handler.beginFile(fn)

      val reader = ds.getFormat().createReader(f.getAbsolutePath(), handler)
      reader.parse()

      handler.endFile(fn)
    }
    handler.quit()
  }

  def handleCompression(options: Options,
    configFilename: String,
    outputFilename: Option[String]) {
    val gson = new Gson()
    val config = gson.fromJson(
        new FileReader(configFilename), classOf[CompressionConfig])

    val ds = DataSet(config.dataSet)
    val fileList = ds.runFileList(options.dataSetDirectory)
    val handler = new CompressionHandler(config, outputFilename)
    processFileHandler(options, ds, fileList, handler)
  }

  def handleChunkIndex(options: Options,
    configFilename: String,
    outputFilename: Option[String],
    ioTraceFilename: Option[String]) {
    val gson = new Gson()
    val config = gson.fromJson(
        new FileReader(configFilename), classOf[ChunkIndexConfig])

    val ds = DataSet(config.dataSet)
    val fileList = ds.runFileList(options.dataSetDirectory)
    val ioTrace = ioTraceFilename match {
      case Some(filename) => new ASCIIDiskSimTrace(filename)
      case None => new NoDiskSimTrace()
    }

    val handler = new ChunkIndexHandler(config, outputFilename, ioTrace)
    processFileHandler(options, ds, fileList, handler)
  }

  def handleEntropy(options: Options,
    configFilename: String,
    outputFilename: Option[String]) {
    val gson = new Gson()
    val config =  gson.fromJson(
        new FileReader(configFilename), classOf[EntropyHandlerConfig]) 

    val ds = DataSet(config.dataSet)
    val fileList = ds.runFileList(options.dataSetDirectory)
    val handler = new EntropyHandler(config, outputFilename)
    processFileHandler(options, ds, fileList, handler)
  }

  def handleContainerCacheType(options: Options,
    configFilename: String,
    outputFilename: Option[String],
    ioTraceFilename: Option[String]) {
    val gson = new Gson()

    val config = gson.fromJson(
        new FileReader(configFilename), classOf[ContainerCacheConfig])

    val ds = DataSet(config.dataSet)
    val fileList = ds.runFileList(options.dataSetDirectory)
    val ioTrace = ioTraceFilename match {
      case Some(filename) => new ASCIIDiskSimTrace(filename)
      case None => new NoDiskSimTrace()
    }
    val handler = new ContainerCacheHandler(config, outputFilename, ioTrace)
    processFileHandler(options, ds, fileList, handler)
  }

  def handleExtremeBinningType(options: Options,
    configFilename: String,
    outputFilename: Option[String],
    ioTraceFilename: Option[String]) {
    val gson = new Gson()

    val config = gson.fromJson(
        new FileReader(configFilename), classOf[ExtremeBinningConfig])

    val ds = DataSet(config.dataSet)
    val fileList = ds.runFileList(options.dataSetDirectory)
    val ioTrace = ioTraceFilename match {
      case Some(filename) => new ASCIIDiskSimTrace(filename)
      case None => new NoDiskSimTrace()
    }
    val handler = new ExtremeBinningHandler(config, outputFilename, ioTrace)
    processFileHandler(options, ds, fileList, handler)
  }

  def handleSparseIndexingType(options: Options,
    configFilename: String,
    outputFilename: Option[String],
    ioTraceFilename: Option[String]) {
    val gson = new Gson()

    val config = gson.fromJson(
        new FileReader(configFilename), classOf[SparseIndexingConfig])

    val ds = DataSet(config.dataSet)
    val fileList = ds.runFileList(options.dataSetDirectory)
    val ioTrace = ioTraceFilename match {
      case Some(filename) => new ASCIIDiskSimTrace(filename)
      case None => new NoDiskSimTrace()
    }
    val handler = new SparseIndexingHandler(config, outputFilename, ioTrace)
    processFileHandler(options, ds, fileList, handler)
  }
  
  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    try {
      import ArgotConverters._

      val parser = new ArgotParser("fs-ca", preUsage = Some("Version 0.1.0"))
      val optionType = parser.option[String](List("t", "type"), "type", "Handler Type")
      val optionDebugging = parser.flag[Boolean]("debug", false, "Debugging output")
      val optionDataDirectory = parser.option[String](List("d", "data"), "data", "Data set directory")
      val optionDiskSimTrace = parser.option[String](List("io-trace"), "io trace", "DiskSim IO trace filename")
      val optionFilename = parser.option[String](List("f", "filename"), "filenames", "Filename to parse")
      val optionOutput = parser.option[String](List("o", "output"), "output", "Output filename")
      val optionReport = parser.option[Int](List("r", "report"), "report", "Interval between progess reports in seconds (Default: 1 minute, 0 = no report)")
      val optionMemoryReporting = parser.flag[Boolean]("report-memory-usage", false, "Report memory usage (expert)")

      parser.parse(args)

      optionDebugging.value match {
        case Some(true) => DOMConfigurator.configure("conf/log4j_debug.xml")
        case _ => DOMConfigurator.configure("conf/log4j.xml")
      }

      val handlerType = optionType.value match {
        case Some(t) => t
        case None => throw new Exception("Provide at least one handler type via -t")
      }
      val reportInterval = optionReport.value
      val reportMemoryUsage = optionMemoryReporting.value match {
        case Some(b) => b
        case None => false
      }
      val dataSetDirectory = optionDataDirectory.value match {
        case None => "data"
        case Some(f) => f
      }

      val memoryUsageReporter = if (reportMemoryUsage) {
        Some(new Reporter(new GCReporting(), reportInterval).start())
      } else {
        None
      }

      val filename = optionFilename.value match {
        case None => throw new Exception("Provide a file with -f")
        case Some(f) => f
      }
     
      val options = new Options(dataSetDirectory, reportInterval)
      handlerType match {
        case "container-cache" =>
          handleContainerCacheType(options, filename, optionOutput.value, optionDiskSimTrace.value)
        case "block-index-filter" =>
          handleBlockIndexFilterType(options, filename, optionOutput.value, optionDiskSimTrace.value)
        case "extreme-binning" =>
          handleExtremeBinningType(options, filename, optionOutput.value, optionDiskSimTrace.value)
        case "sparse-indexing" =>
          handleSparseIndexingType(options, filename, optionOutput.value, optionDiskSimTrace.value)
        case "entropy" =>
          handleEntropy(options, filename, optionOutput.value)
        case "compression" =>
          handleCompression(options, filename, optionOutput.value)
        case "chunk-index" =>
          handleChunkIndex(options, filename, optionOutput.value, optionDiskSimTrace.value)
      }

      memoryUsageReporter match {
        case Some(r) => r.quit()
        case None => //pass
      }

    } catch {
      case e: SystemExitException => System.exit(1)
    }
  }

  private def processFileHandler(options: Options, ds: DataSet, fileList: List[(java.io.File, Int)], handler: DefaultFileDataHandler): Unit = {

    val reporter = new Reporter(handler, options.reportInterval).start()
    for ((f, fn) <- fileList) {
      handler.beginFile(fn)

      val reader = ds.getFormat().createReader(f.getAbsolutePath(), handler)
      reader.parse()

      handler.endFile(fn)
    }
    reporter.quit()
    handler.quit()
  }
}
