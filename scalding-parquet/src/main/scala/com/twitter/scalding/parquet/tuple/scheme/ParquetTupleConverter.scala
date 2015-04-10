package com.twitter.scalding.parquet.tuple.scheme

import parquet.io.api.{ Binary, Converter, GroupConverter, PrimitiveConverter }

import scala.collection.mutable
import scala.util.Try

/**
 * Parquet tuple converter used to create user defined tuple value from parquet column values
 * @param parent parent parquet tuple converter
 * @param isOption is the field optional
 * @param outerIndex field index in parent tuple schema
 */
abstract class ParquetTupleConverter(val parent: Option[ParquetTupleConverter] = None, val isOption: Boolean = false,
  val outerIndex: Int = -1) extends GroupConverter {
  var converters: Map[Int, Converter] = Map()
  val fieldValues: mutable.ArrayBuffer[Any] = mutable.ArrayBuffer()

  var currentValue: Any = null

  def createValue(): Any

  def newConverter(i: Int): Converter

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
    currentValue = createValue()
    fieldValues.remove(0, fieldValues.size)
    parent.map(_.addFieldValue(outerIndex, currentValue, isOption))
  }

  override def start(): Unit = ()

  def addFieldValue(index: Int, value: Any, isOpt: Boolean) = {
    val currentSize = fieldValues.size
    //insert none for these optional fields that has non value written for given row
    (currentSize until index).map(fieldValues.insert(_, None))
    if (isOpt) fieldValues.insert(index, Option(value)) else fieldValues.insert(index, value)
  }
}

class PrimitiveTypeConverter(val index: Int, val parent: ParquetTupleConverter, val isOption: Boolean)
  extends PrimitiveConverter {
  def appendValue(value: Any) = parent.addFieldValue(index, value, isOption)
}

class StringConverter(index: Int, parent: ParquetTupleConverter, isOption: Boolean = false)
  extends PrimitiveTypeConverter(index, parent, isOption) {
  override def addBinary(value: Binary): Unit = appendValue(value.toStringUsingUTF8)
}

class DoubleConverter(index: Int, parent: ParquetTupleConverter, isOption: Boolean = false)
  extends PrimitiveTypeConverter(index, parent, isOption) {
  override def addDouble(value: Double): Unit = appendValue(value)
}

class FloatConverter(index: Int, parent: ParquetTupleConverter, isOption: Boolean = false)
  extends PrimitiveTypeConverter(index, parent, isOption) {
  override def addFloat(value: Float): Unit = appendValue(value)
}

class LongConverter(index: Int, parent: ParquetTupleConverter, isOption: Boolean = false)
  extends PrimitiveTypeConverter(index, parent, isOption) {
  override def addLong(value: Long) = appendValue(value)
}

class IntConverter(index: Int, parent: ParquetTupleConverter, isOption: Boolean = false)
  extends PrimitiveTypeConverter(index, parent, isOption) {
  override def addInt(value: Int) = appendValue(value)
}

class ShortConverter(index: Int, parent: ParquetTupleConverter, isOption: Boolean = false)
  extends PrimitiveTypeConverter(index, parent, isOption) {
  override def addInt(value: Int) = appendValue(Try(value.toShort).getOrElse(null))
}

class ByteConverter(index: Int, parent: ParquetTupleConverter, isOption: Boolean = false)
  extends PrimitiveTypeConverter(index, parent, isOption) {
  override def addInt(value: Int) = appendValue(Try(value.toByte).getOrElse(null))
}

class BooleanConverter(index: Int, parent: ParquetTupleConverter, isOption: Boolean = false)
  extends PrimitiveTypeConverter(index, parent, isOption) {
  override def addBoolean(value: Boolean) = appendValue(value)
}