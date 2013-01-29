/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import de.pc2.dedup.chunker.Digest
import de.pc2.dedup.util.Log

/**
 * Statistics about the chunk cache
 */
class ChunkCacheStatistics(var hits: Int, var misses: Int, var hitRatio: Double) {
    def this() = this(0, 0, 0.0)
    
    def finish() {
        hitRatio = if (hits + misses == 0) {
            0.0
        } else {
            1.0 * hits / (1.0 * (hits + misses))
        }
    }
}

/**
 * An LRU chunk cache
 */
class ChunkCache(chunkCacheSize: Int, chunkIndex: ChunkIndex) extends Log {
    val cache : LRU[Digest, Int] = if (chunkCacheSize > 0) {
        new LRU[Digest, Int](chunkCacheSize)
    } else {
        null
    }
    var stats = new ChunkCacheStatistics()    

    
    def finish() {     
        if (cache != null) {
            cache.clear()
        }
           
        stats = new ChunkCacheStatistics()        
    }
    
    def contains(fp: Digest) : Boolean = {
        if (cache == null) {
            false
        } else {
            val r = cache.contains(fp)
            if (r) {
                cache.update(fp, 1)
                stats.hits += 1
            } else {
                stats.misses += 1
            }
            r
        }
    }
    
    def update(fp: Digest) {
        if (cache != null) {
            cache.update(fp, 1)
        }
    }
}