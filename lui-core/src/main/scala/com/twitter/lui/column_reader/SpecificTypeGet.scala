package com.twitter.lui.column_reader
import java.nio.ByteBuffer

trait HasCache {
  protected def storeVToCache(position: Int): Unit
  protected def cachePrimitivePosition: Array[Int]
}

trait SpecificTypeGet {
  def getInteger(): Int = {
    throw new UnsupportedOperationException()
  }

  def getBoolean(): Boolean = {
    throw new UnsupportedOperationException()
  }

  def getLong(): Long = {
    throw new UnsupportedOperationException()
  }

  def getBinary(): ByteBuffer = {
    throw new UnsupportedOperationException()
  }

  def getString(): String = {
    throw new UnsupportedOperationException()
  }

  def getFloat(): Float = {
    throw new UnsupportedOperationException()
  }

  def getDouble(): Double = {
    throw new UnsupportedOperationException()
  }
}