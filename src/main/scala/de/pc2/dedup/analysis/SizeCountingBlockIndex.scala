/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import de.pc2.dedup.util.Log

class SizeCountingBlockIndexStatistics {
  var fingerprintCount : Long = 0
  var codeWordFingerprintCount: Long = 0
  var fullFingerprintDigestSize : Long = 0
  var digestSize : Long = 0
  var blockCount : Long = 0
  
  var compressionRatio : Double = Double.NaN

  def finish() {
    if (fullFingerprintDigestSize > 0) {
      compressionRatio = 1.0 - (1.0 * digestSize / fullFingerprintDigestSize)
    }
  }
}

class SizeCountingBlockIndex(val fullDigestLength: Int) extends Log {

    def add(blockId: Int, bm: BlockMappingList) {
        for (d <- bm.items) {
          stats.fingerprintCount += 1
          stats.fullFingerprintDigestSize += fullDigestLength

          val l = d match {
            case cwd: CodeWordDigest => 
              if (cwd.codeWordLength < fullDigestLength) {
            	  stats.codeWordFingerprintCount += 1
              }
              cwd.codeWordLength
            case _ => fullDigestLength
          }
          stats.digestSize += l
        }
        stats.blockCount += 1
    }
    
    var stats = new SizeCountingBlockIndexStatistics()
    
    def finish() {
        val oldStats = stats
        stats = new SizeCountingBlockIndexStatistics()
        stats.fingerprintCount = oldStats.fingerprintCount
        stats.codeWordFingerprintCount = oldStats.codeWordFingerprintCount
        stats.fullFingerprintDigestSize = oldStats.fullFingerprintDigestSize
        stats.digestSize = oldStats.digestSize
        stats.blockCount = oldStats.blockCount 
    }
}