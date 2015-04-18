package com.twitter.scalding.parquet.tuple.scheme

import parquet.io.api.{ Binary, Converter, GroupConverter, PrimitiveConverter }
import scala.util.Try

trait TupleFieldConverter[+T] extends Converter {
  /**
   * Current value read from parquet column
   */
  def currentValue: T

  /**
   * reset the converter state, make it ready for reading next column value.
   */
  def reset(): Unit
}

/**
 * Parquet tuple converter used to create user defined tuple value from parquet column values
 */
abstract class ParquetTupleConverter[T] extends GroupConverter with TupleFieldConverter[T] {
  override def start(): Unit = reset()
  override def end(): Unit = ()
}

abstract class OptionalParquetTupleConverter[T] extends GroupConverter with TupleFieldConverter[Option[T]] {
  var value: Option[T] = None
  val delegate: ParquetTupleConverter[T]

  def currentValue: Option[T] = value

  override def start(): Unit = reset()

  override def getConverter(i: Int): Converter = delegate.getConverter(i)

  override def reset(): Unit = {
    value = None
    delegate.reset()
  }

  override def end(): Unit = {
    value = Option(delegate.currentValue)
  }
}

trait PrimitiveFieldConverter[T] extends PrimitiveConverter with TupleFieldConverter[T] {
  val defaultValue: T
  var value: T = defaultValue

  override def currentValue: T = value

  override def reset(): Unit = value = defaultValue
}

abstract class OptionalPrimitiveFieldConverter[T] extends PrimitiveConverter with TupleFieldConverter[Option[T]] {
  var value: Option[T] = None

  val delegate: PrimitiveFieldConverter[T]

  override def reset(): Unit = {
    value = None
    delegate.reset()
  }

  override def currentValue: Option[T] = value

  override def addBinary(v: Binary) = {
    delegate.addBinary(v)
    value = Option(delegate.currentValue)
  }

  override def addBoolean(v: Boolean) = {
    delegate.addBoolean(v)
    value = Option(delegate.currentValue)
  }

  override def addDouble(v: Double) = {
    delegate.addDouble(v)
    value = Option(delegate.currentValue)
  }

  override def addFloat(v: Float) = {
    delegate.addFloat(v)
    value = Option(delegate.currentValue)
  }

  override def addInt(v: Int) = {
    delegate.addInt(v)
    value = Option(delegate.currentValue)
  }

  override def addLong(v: Long) = {
    delegate.addLong(v)
    value = Option(delegate.currentValue)
  }
}

class StringConverter extends PrimitiveFieldConverter[String] {
  override val defaultValue: String = null

  override def addBinary(binary: Binary): Unit = value = binary.toStringUsingUTF8
}

class DoubleConverter extends PrimitiveFieldConverter[Double] {
  override val defaultValue: Double = 0D

  override def addDouble(v: Double): Unit = value = v
}

class FloatConverter extends PrimitiveFieldConverter[Float] {
  override val defaultValue: Float = 0F

  override def addFloat(v: Float): Unit = value = v
}

class LongConverter extends PrimitiveFieldConverter[Long] {
  override val defaultValue: Long = 0L

  override def addLong(v: Long): Unit = value = v
}

class IntConverter extends PrimitiveFieldConverter[Int] {
  override val defaultValue: Int = 0

  override def addInt(v: Int): Unit = value = v
}

class ShortConverter extends PrimitiveFieldConverter[Short] {
  override val defaultValue: Short = 0

  override def addInt(v: Int): Unit = value = Try(v.toShort).getOrElse(0)
}

class ByteConverter extends PrimitiveFieldConverter[Byte] {
  override val defaultValue: Byte = 0

  override def addInt(v: Int): Unit = value = Try(v.toByte).getOrElse(0)
}

class BooleanConverter extends PrimitiveFieldConverter[Boolean] {
  override val defaultValue: Boolean = false

  override def addBoolean(v: Boolean): Unit = value = v
}
