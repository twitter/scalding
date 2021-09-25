package com.twitter.scalding.parquet.tuple.scheme

import org.apache.parquet.io.api.{ Binary, Converter, GroupConverter, PrimitiveConverter }
import scala.util.Try

trait TupleFieldConverter[+T] extends Converter with Serializable {
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

/**
 * Primitive fields converter
 * @tparam T primitive types (String, Double, Float, Long, Int, Short, Byte, Boolean)
 */
trait PrimitiveFieldConverter[T] extends PrimitiveConverter with TupleFieldConverter[T] {
  val defaultValue: T
  var value: T = defaultValue

  override def currentValue: T = value

  override def reset(): Unit = value = defaultValue
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

/**
 * Collection field converter, such as list(Scala Option is also seen as a collection).
 * @tparam T collection element type(can be primitive types or nested types)
 */
trait CollectionConverter[T] {
  val child: TupleFieldConverter[T]

  def appendValue(v: T): Unit
}

/**
 * A wrapper of primitive converters for modeling primitive fields in a collection
 * @tparam T primitive types (String, Double, Float, Long, Int, Short, Byte, Boolean)
 */
abstract class CollectionElementPrimitiveConverter[T](val parent: CollectionConverter[T]) extends PrimitiveConverter
  with TupleFieldConverter[T] {
  val delegate: PrimitiveFieldConverter[T]

  override def addBinary(v: Binary) = {
    delegate.addBinary(v)
    parent.appendValue(delegate.currentValue)
  }

  override def addBoolean(v: Boolean) = {
    delegate.addBoolean(v)
    parent.appendValue(delegate.currentValue)
  }

  override def addDouble(v: Double) = {
    delegate.addDouble(v)
    parent.appendValue(delegate.currentValue)
  }

  override def addFloat(v: Float) = {
    delegate.addFloat(v)
    parent.appendValue(delegate.currentValue)
  }

  override def addInt(v: Int) = {
    delegate.addInt(v)
    parent.appendValue(delegate.currentValue)
  }

  override def addLong(v: Long) = {
    delegate.addLong(v)
    parent.appendValue(delegate.currentValue)
  }

  override def currentValue: T = delegate.currentValue

  override def reset(): Unit = delegate.reset()
}

/**
 * A wrapper of group converters for modeling group type element in a collection
 * @tparam T group tuple type(can be a collection type, such as list)
 */
abstract class CollectionElementGroupConverter[T](val parent: CollectionConverter[T]) extends GroupConverter
  with TupleFieldConverter[T] {

  val delegate: TupleFieldConverter[T]

  override def getConverter(i: Int): Converter = delegate.asGroupConverter().getConverter(i)

  override def end(): Unit = {
    parent.appendValue(delegate.currentValue)
    delegate.asGroupConverter().end()
  }

  override def start(): Unit = delegate.asGroupConverter().start()

  override def currentValue: T = delegate.currentValue

  override def reset(): Unit = delegate.reset()
}

/**
 * Option converter for modeling option field
 * @tparam T option element type(can be primitive types or nested types)
 */
abstract class OptionConverter[T] extends TupleFieldConverter[Option[T]] with CollectionConverter[T] {
  var value: Option[T] = None

  override def appendValue(v: T): Unit = value = Option(v)

  override def currentValue: Option[T] = value

  override def reset(): Unit = {
    value = None
    child.reset()
  }

  override def isPrimitive: Boolean = child.isPrimitive

  override def asGroupConverter: GroupConverter = child.asGroupConverter()

  override def asPrimitiveConverter: PrimitiveConverter = child.asPrimitiveConverter()
}

/**
 * List in parquet is represented by 3-level structure.
 * Check this https://github.com/apache/incubator-parquet-format/blob/master/LogicalTypes.md
 * Helper class to wrap a converter for a list group converter
 */
object ListElement {
  def wrapper(child: Converter): GroupConverter = new GroupConverter() {
    override def getConverter(i: Int): Converter = {
      if (i != 0)
        throw new IllegalArgumentException("list have only one element field. can't reach " + i)
      child
    }

    override def end(): Unit = ()

    override def start(): Unit = ()
  }
}
/**
 * List converter for modeling list field
 * @tparam T list element type(can be primitive types or nested types)
 */
abstract class ListConverter[T] extends GroupConverter with TupleFieldConverter[List[T]] with CollectionConverter[T] {

  var value: List[T] = Nil

  def appendValue(v: T): Unit = value = value :+ v

  lazy val listElement: GroupConverter = new GroupConverter() {
    override def getConverter(i: Int): Converter = {
      if (i != 0)
        throw new IllegalArgumentException("lists have only one element field. can't reach " + i)
      child
    }

    override def end(): Unit = ()

    override def start(): Unit = ()
  }

  override def getConverter(i: Int): Converter = {
    if (i != 0)
      throw new IllegalArgumentException("lists have only one element field. can't reach " + i)
    listElement
  }

  override def end(): Unit = ()

  override def start(): Unit = reset()

  override def currentValue: List[T] = value

  override def reset(): Unit = {
    value = Nil
    child.reset()
  }
}

/**
 * Set converter for modeling set field
 * @tparam T list element type(can be primitive types or nested types)
 */
abstract class SetConverter[T] extends GroupConverter with TupleFieldConverter[Set[T]] with CollectionConverter[T] {

  var value: Set[T] = Set()

  def appendValue(v: T): Unit = value = value + v

  //in the back end set is stored as list
  lazy val listElement: GroupConverter = ListElement.wrapper(child)

  override def getConverter(i: Int): Converter = {
    if (i != 0)
      throw new IllegalArgumentException("sets have only one element field. can't reach " + i)
    listElement
  }

  override def end(): Unit = ()

  override def start(): Unit = reset()

  override def currentValue: Set[T] = value

  override def reset(): Unit = {
    value = Set()
    child.reset()
  }
}

/**
 * Map converter for modeling map field
 * @tparam K map key type
 * @tparam V map value type
 */
abstract class MapConverter[K, V] extends GroupConverter with TupleFieldConverter[Map[K, V]] with CollectionConverter[(K, V)] {

  var value: Map[K, V] = Map()

  def appendValue(v: (K, V)): Unit = value = value + v

  override def getConverter(i: Int): Converter = {
    if (i != 0)
      throw new IllegalArgumentException("maps have only one element type key_value(0). can't reach " + i)
    child
  }

  override def end(): Unit = ()

  override def start(): Unit = reset()

  override def currentValue: Map[K, V] = value

  override def reset(): Unit = {
    value = Map()
    child.reset()
  }
}

abstract class MapKeyValueConverter[K, V](parent: CollectionConverter[(K, V)])
  extends CollectionElementGroupConverter[(K, V)](parent) {

  val keyConverter: TupleFieldConverter[K]

  val valueConverter: TupleFieldConverter[V]

  override lazy val delegate: TupleFieldConverter[(K, V)] = new GroupConverter with TupleFieldConverter[(K, V)] {
    override def currentValue: (K, V) = (keyConverter.currentValue, valueConverter.currentValue)

    override def reset(): Unit = {
      keyConverter.reset()
      valueConverter.reset()
    }

    override def getConverter(i: Int): Converter = {
      if (i == 0) keyConverter
      else if (i == 1) valueConverter
      else throw new IllegalArgumentException("key_value has only the key (0) and value (1) fields expected: " + i)
    }

    override def end(): Unit = ()

    override def start(): Unit = reset()
  }
}

