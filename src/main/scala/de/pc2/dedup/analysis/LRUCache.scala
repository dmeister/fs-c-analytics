/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConversions.asScalaSet

import de.pc2.dedup.util.LRUMap2
import de.pc2.dedup.util.LRURemoveListener

/**
 * LRU Cache wrapping {@link org.apache.commons.collections.map.LRUMap}
 *
 * @param size the maximum number of Elements allowed in the LRU map
 * @param loadFactor the Load Factor to construct our LRU with.
 */
class LRU[KeyType, ValueType](size: Int, callback: (KeyType, ValueType) => Boolean) {
  // from http://stackoverflow.com/questions/6483262/generically-implementing-a-java-single-abstract-method-interface-with-a-scala-cl
  implicit def func2LRURemoveListener(f: (Any, Any) => Boolean): LRURemoveListener = {
    new LRURemoveListener {
      def removeLRU(k: Any, v: Any): Boolean = f(k, v)
    }
  }
  def ic(k: Any, v: Any): Boolean = {
    if (callback != null) {
      return callback(k.asInstanceOf[KeyType], v.asInstanceOf[ValueType])
    } else {
      return true;
    }
  }
  def this(size: Int) = this(size, null)

  private val map = new LRUMap2(size, (k: Any, v: Any) => ic(k, v))

  def clear() {
    map.clear()
  }

  def update(k: KeyType, v: ValueType) {
    map.put(k, v)
  }

  def remove(k: KeyType) = map.remove(k)

  def apply(k: KeyType): ValueType = map.get(k).asInstanceOf[ValueType]
  def contains(k: KeyType): Boolean = map.containsKey(k)
  def keys: List[KeyType] = map.keySet().toList.map(_.asInstanceOf[KeyType])

  def orderedKeys: List[KeyType] = map.orderedMapIterator().toList.map(_.asInstanceOf[KeyType])
  
  override def toString = {
    map.toString()
  }
}