package com.twitter.scalding.parquet.tuple.scheme

import parquet.io.api.{ Binary, Converter, GroupConverter, PrimitiveConverter }
import scala.util.Try

/**
 * Parquet tuple field converter used by parquet read support to read tuple field value from parquet.
 * @tparam T field value type.
 */
trait TupleFieldConverter[+T] extends Converter {
  /**
   * Current value read from parquet column
   */
  def currentValue: T

  /**
   * Used to check if a optional field has value stored or not.
   */
  var hasValue: Boolean = false

  /**
   *reset the converter state, make it ready for reading next column value.
   */
  def reset(): Unit
}

/**
 * Parquet tuple converter used to create user defined tuple value from parquet column values
 * @param parent parent parquet tuple converter
 */
abstract class ParquetTupleConverter(val parent: Option[ParquetTupleConverter] = None) extends GroupConverter
  with TupleFieldConverter[Any] {
  var converters: Map[Int, TupleFieldConverter[Any]] = Map()

  var value: Any = null

  override def currentValue: Any = value

  def createValue(): Any

  def newConverter(i: Int): TupleFieldConverter[Any]

  override def getConverter(i: Int) = {
    val converter = converters.get(i)
    if (converter.isDefined) converter.get
    else {
      val c = newConverter(i)
      converters += i -> c
      c
    }
  }

  override def end(): Unit = {
    if (hasValue) {
      value = createValue()
      parent.map(p => p.hasValue = true)
    }
  }

  override def reset(): Unit = {
    value = null
    converters.values.map(v => v.reset())
    hasValue = false
  }

  override def start(): Unit = reset()
}

sealed trait PrimitiveTupleFieldConverter[T] extends TupleFieldConverter[T] {
  val parent: ParquetTupleConverter
  val defaultValue: T
  var value: T = defaultValue

  override def currentValue: T = value

  protected def valueAdded(): Unit = {
    hasValue = true
    parent.hasValue = true
  }

  override def reset(): Unit = {
    value = defaultValue
    hasValue = false
  }
}

case class StringConverter(parent: ParquetTupleConverter) extends PrimitiveConverter with PrimitiveTupleFieldConverter[String] {
  override val defaultValue: String = null

  override def addBinary(binary: Binary): Unit = {
    value = binary.toStringUsingUTF8
    valueAdded()
  }
}

case class DoubleConverter(parent: ParquetTupleConverter) extends PrimitiveConverter with PrimitiveTupleFieldConverter[Double] {
  override val defaultValue: Double = 0D

  override def addDouble(v: Double): Unit = {
    value = v
    valueAdded()
  }
}

case class FloatConverter(parent: ParquetTupleConverter) extends PrimitiveConverter with PrimitiveTupleFieldConverter[Float] {
  override val defaultValue: Float = 0F

  override def addFloat(v: Float): Unit = {
    value = v
    valueAdded()
  }
}

case class LongConverter(parent: ParquetTupleConverter) extends PrimitiveConverter with PrimitiveTupleFieldConverter[Long] {
  override val defaultValue: Long = 0L

  override def addLong(v: Long): Unit = {
    value = v
    valueAdded()
  }
}

case class IntConverter(parent: ParquetTupleConverter) extends PrimitiveConverter with PrimitiveTupleFieldConverter[Int] {
  override val defaultValue: Int = 0

  override def addInt(v: Int): Unit = {
    value = v
    valueAdded()
  }
}

case class ShortConverter(parent: ParquetTupleConverter) extends PrimitiveConverter with PrimitiveTupleFieldConverter[Short] {
  override val defaultValue: Short = 0

  override def addInt(v: Int): Unit = {
    value = Try(v.toShort).getOrElse(0)
    valueAdded()
  }
}

case class ByteConverter(parent: ParquetTupleConverter) extends PrimitiveConverter with PrimitiveTupleFieldConverter[Byte] {
  override val defaultValue: Byte = 0

  override def addInt(v: Int): Unit = {
    value = Try(v.toByte).getOrElse(0)
    valueAdded()
  }
}

case class BooleanConverter(parent: ParquetTupleConverter) extends PrimitiveConverter with PrimitiveTupleFieldConverter[Boolean] {
  override val defaultValue: Boolean = false

  override def addBoolean(v: Boolean): Unit = {
    value = v
    valueAdded()
  }
}
