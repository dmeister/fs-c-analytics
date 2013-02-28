/*
 * fsc-analytics
 */
package de.pc2.dedup.util

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
import de.pc2.dedup.fschunk.GCReporting
import de.pc2.dedup.analysis.ChunkIndexConfig
import de.pc2.dedup.analysis.ChunkIndexHandler
import de.pc2.dedup.analysis.SparseIndexingConfig
import de.pc2.dedup.analysis.SparseIndexingHandler
import de.pc2.dedup.compression.EntropyHandlerConfig
import de.pc2.dedup.compression.EntropyHandler
import de.pc2.dedup.chunker.FilePart
import de.pc2.dedup.chunker.File
import de.pc2.dedup.DefaultFileDataHandler
import de.pc2.dedup.chunker.Chunk
import de.pc2.dedup.fschunk.format.Format
import java.io.FileInputStream
import java.io.FileOutputStream
import de.pc2.dedup.fschunk.handler.FileDataHandler
import de.pc2.dedup.chunker.DigestFactory
import java.nio.CharBuffer
import java.nio.charset.Charset
import de.pc2.dedup.fschunk.trace.PrivacyMode

class RehashOptions(val sourceFormat: Format,
  val outputFormat: Format,
  val digestFactory: DigestFactory,
  val reportInterval: Option[Int]) {
}

/**
 * Main object for the parser.
 * The parser is used to replay trace of chunking runs and to re-hash (salt) the
 * trace fingerprint to finally write a different trace file.
 *
 * The rehashing further anonyomizes the data set.
 */
object RehashMain {

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    try {
      import ArgotConverters._

      val parser = new ArgotParser("fs-ca rehash", preUsage = Some("Version 0.1.0"))
      val optionDebugging = parser.flag[Boolean]("debug", false, "Debugging output")

      val optionFormat = parser.option[String](List("format"), "trace file format", "Trace file format (expert)")
      val optionOutputFormat = parser.option[String](List("output-format"), "trace output file format", "Trace file output format (expert)")
      val optionFilename = parser.option[String](List("f", "filename"), "filenames", "Filename to parse")
      val optionOutput = parser.option[String](List("o", "output"), "output", "Output filename")
      val optionReport = parser.option[Int](List("r", "report"), "report", "Interval between progess reports in seconds (Default: 1 minute, 0 = no report)")
      val optionMemoryReporting = parser.flag[Boolean]("report-memory-usage", false, "Report memory usage (expert)")
      val optionSalt = parser.option[String](List("salt"), "salt", "Salt the fingerprints")
      val optionDigestLength = parser.option[Int]("digest-length", "digestLength", "Length of Digest (Fingerprint)")
      val optionDigestType = parser.option[String]("digest-type", "digestType", "Type of Digest (Fingerprint)")

      parser.parse(args)

      optionDebugging.value match {
        case Some(true) => DOMConfigurator.configure("conf/log4j_debug.xml")
        case _ => DOMConfigurator.configure("conf/log4j.xml")
      }
      val salt = optionSalt.value match {
        case Some(s) => s
        case None => throw new Exception("Provide a salt value with -s")
      }
      val digestLength = optionDigestLength.value match {
        case Some(l) => l
        case None => 20
      }
      val digestType = optionDigestType.value match {
        case Some(t) => t
        case None => "SHA-1"
      }
      val sourceFormat = optionFormat.value match {
        case Some(s) =>
          if (!Format.isFormat(s)) {
            parser.usage("Invalid fs-c file format")
          }
          Format(s)
        case None => Format("protobuf")
      }
      val outputFormat = optionOutputFormat.value match {
        case Some(s) =>
          if (!Format.isFormat(s)) {
            parser.usage("Invalid fs-c file format")
          }
          Format(s)
        case None => Format("protobuf")
      }
      val reportInterval = optionReport.value
      val reportMemoryUsage = optionMemoryReporting.value match {
        case Some(b) => b
        case None => false
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
      val output = optionOutput.value match {
        case None => throw new Exception("Provide a output file with -o")
        case Some(f) => f
      }

      val digestFactory = new DigestFactory(digestType, digestLength, Some(salt))
      val options = new RehashOptions(sourceFormat, outputFormat, digestFactory, reportInterval)
      rehash(options, filename, output)

      memoryUsageReporter match {
        case Some(r) => r.quit()
        case None => //pass
      }

    } catch {
      case e: SystemExitException => System.exit(1)
    }
  }

  class RehashDataHandler(options: RehashOptions, writer: FileDataHandler) extends FileDataHandler with Log {
    private val digestBuilder = options.digestFactory.builder()
    private val charset = Charset.forName("UTF-8")
    private val encoder = charset.newEncoder()
    private val decoder = charset.newDecoder()

    def rehashChunk(c: Chunk): Chunk = {
      digestBuilder.append(c.fp.digest, 0, c.fp.digest.length)
      Chunk(c.size, digestBuilder.build(), None)

    }
    def rehashFilename(f: String): String = {
      val rehashedBuffer = digestBuilder.append(encoder.encode(CharBuffer.wrap(f))).build()
      org.apache.commons.codec.binary.Base64.encodeBase64String(rehashedBuffer.digest)
    }

    def handle(f: File) {
      logger.debug("%s".format(f.filename))
      val newChunks = for (c <- f.chunks) yield rehashChunk(c)
      val newFile = new File(rehashFilename(f.filename), f.fileSize, f.fileType, newChunks, None)
      writer.handle(newFile)
    }

    def handle(fp: FilePart) {
      val newChunks = for (c <- fp.chunks) yield rehashChunk(c)
      val newFilePart = new FilePart(rehashFilename(fp.filename), newChunks)
      writer.handle(newFilePart)
    }

    override def quit() {
      logger.info("Source run finished")
    }
  }

  private def rehash(options: RehashOptions, sourceFilename: String, outputFilename: String): Unit = {
    // PrivacyMode.NoPrivacy is ok here, as the filename is internally rehashed using SHA-1 and a salt before
    val writer = options.outputFormat.createWriter(new FileOutputStream(outputFilename), PrivacyMode.NoPrivacy)
    val handler = new RehashDataHandler(options, writer)
    val reader = options.sourceFormat.createReader(new FileInputStream(sourceFilename), handler)

    val reporter = new Reporter(handler, options.reportInterval).start()
    reader.parse()
      
    reporter.quit()
    handler.quit()
    writer.quit()
  }
}
