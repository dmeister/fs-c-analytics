/*
 * fsc-analytics
 */
package de.pc2.dedup.analysis

import scala.collection.mutable.Map

trait FrequencyEstimator[T] {
  def add(value: T)
  def apply(value: T): Int
  def getMaxValue(): Option[T]
}

class MapFrequenceEstimator[T] extends FrequencyEstimator[T] {
  val map = Map.empty[T, Int]

  def add(value: T) {
    if (map.contains(value)) {
      map(value) = map(value) + 1
    } else {
      map(value) = 1
    }
  }

  def apply(value: T): Int = {
    if (map.contains(value)) {
      map(value)
    } else {
      0
    }
  }

  def getMaxValue(): Option[T] = {
    if (map.isEmpty) {
      None
    } else {
      val m = map.maxBy(_._2)
      Some(m._1)
    }
  }
}

class MisraGries[T](val k: Int) extends FrequencyEstimator[T] {
  if (k <= 0) {
    throw new IllegalArgumentException("k")
  }
  val map = Map.empty[T, Int]

  override def toString =
    "[k %s, %s]".format(k, map)

  def add(value: T) {
    if (map.contains(value))
      map(value) = map(value) + 1
    else if (map.size < k)
      map(value) = 1
    else {
      for (key <- map.keys.toList) {
        val newValue = map(key) - 1
        if (newValue > 0)
          map(key) = newValue
        else
          map -= key
      }
    }
  }

  def apply(value: T): Int =
    if (map.contains(value))
      map(value)
    else
      0

  def getMaxValue(): Option[T] = {
    if (map.isEmpty) {
      None
    } else {
      val m = map.maxBy(_._2)
      Some(m._1)
    }
  }
}