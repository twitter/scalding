/**
 * Generated by Scrooge
 *   version: ?
 *   rev: ?
 *   built at: ?
 */
package com.twitter.scalding.thrift.macros.scalathrift

import java.nio.ByteBuffer
import java.util.Arrays

import com.twitter.scrooge.{ TFieldBlob, ThriftStruct, ThriftStructCodec3, ThriftStructFieldInfo }
import org.apache.thrift.protocol._
import org.apache.thrift.transport.TMemoryBuffer

import scala.collection.Map
import scala.collection.immutable.{ Map => immutable$Map }
import scala.collection.mutable.{ ArrayBuffer => mutable$ArrayBuffer, Buffer => mutable$Buffer, Builder, HashMap => mutable$HashMap, HashSet => mutable$HashSet }

object TestTypes extends ThriftStructCodec3[TestTypes] {
  private val NoPassthroughFields = immutable$Map.empty[Short, TFieldBlob]
  val Struct = new TStruct("TestTypes")
  val ABoolField = new TField("a_bool", TType.BOOL, 1)
  val ABoolFieldManifest = implicitly[Manifest[Boolean]]
  val AByteField = new TField("a_byte", TType.BYTE, 2)
  val AByteFieldManifest = implicitly[Manifest[Byte]]
  val AI16Field = new TField("a_i16", TType.I16, 3)
  val AI16FieldManifest = implicitly[Manifest[Short]]
  val AI32Field = new TField("a_i32", TType.I32, 4)
  val AI32FieldManifest = implicitly[Manifest[Int]]
  val AI64Field = new TField("a_i64", TType.I64, 5)
  val AI64FieldManifest = implicitly[Manifest[Long]]
  val ADoubleField = new TField("a_double", TType.DOUBLE, 6)
  val ADoubleFieldManifest = implicitly[Manifest[Double]]
  val AStringField = new TField("a_string", TType.STRING, 7)
  val AStringFieldManifest = implicitly[Manifest[String]]
  val AEnumField = new TField("a_enum", TType.ENUM, 8)
  val AEnumFieldI32 = new TField("a_enum", TType.I32, 8)
  val AEnumFieldManifest = implicitly[Manifest[TestEnum]]
  val ABinaryField = new TField("a_binary", TType.STRING, 9)
  val ABinaryFieldManifest = implicitly[Manifest[ByteBuffer]]

  /**
   * Field information in declaration order.
   */
  lazy val fieldInfos: scala.List[ThriftStructFieldInfo] = scala.List[ThriftStructFieldInfo](
    new ThriftStructFieldInfo(
      ABoolField,
      false,
      false,
      ABoolFieldManifest,
      None,
      None,
      immutable$Map(),
      immutable$Map()),
    new ThriftStructFieldInfo(
      AByteField,
      false,
      false,
      AByteFieldManifest,
      None,
      None,
      immutable$Map(),
      immutable$Map()),
    new ThriftStructFieldInfo(
      AI16Field,
      false,
      false,
      AI16FieldManifest,
      None,
      None,
      immutable$Map(),
      immutable$Map()),
    new ThriftStructFieldInfo(
      AI32Field,
      false,
      false,
      AI32FieldManifest,
      None,
      None,
      immutable$Map(),
      immutable$Map()),
    new ThriftStructFieldInfo(
      AI64Field,
      false,
      false,
      AI64FieldManifest,
      None,
      None,
      immutable$Map(),
      immutable$Map()),
    new ThriftStructFieldInfo(
      ADoubleField,
      false,
      false,
      ADoubleFieldManifest,
      None,
      None,
      immutable$Map(),
      immutable$Map()),
    new ThriftStructFieldInfo(
      AStringField,
      false,
      false,
      AStringFieldManifest,
      None,
      None,
      immutable$Map(),
      immutable$Map()),
    new ThriftStructFieldInfo(
      AEnumField,
      false,
      false,
      AEnumFieldManifest,
      None,
      None,
      immutable$Map(),
      immutable$Map()),
    new ThriftStructFieldInfo(
      ABinaryField,
      false,
      false,
      ABinaryFieldManifest,
      None,
      None,
      immutable$Map(),
      immutable$Map()))

  lazy val structAnnotations: immutable$Map[String, String] =
    immutable$Map[String, String]()

  /**
   * Checks that all required fields are non-null.
   */
  def validate(_item: TestTypes) {
  }

  def withoutPassthroughFields(original: TestTypes): TestTypes =
    new Immutable(
      aBool =
        {
          val field = original.aBool
          field
        },
      aByte =
        {
          val field = original.aByte
          field
        },
      aI16 =
        {
          val field = original.aI16
          field
        },
      aI32 =
        {
          val field = original.aI32
          field
        },
      aI64 =
        {
          val field = original.aI64
          field
        },
      aDouble =
        {
          val field = original.aDouble
          field
        },
      aString =
        {
          val field = original.aString
          field
        },
      aEnum =
        {
          val field = original.aEnum
          field
        },
      aBinary =
        {
          val field = original.aBinary
          field
        })

  override def encode(_item: TestTypes, _oproto: TProtocol) {
    _item.write(_oproto)
  }

  override def decode(_iprot: TProtocol): TestTypes = {
    var aBool: Boolean = false
    var aByte: Byte = 0
    var aI16: Short = 0
    var aI32: Int = 0
    var aI64: Long = 0L
    var aDouble: Double = 0.0
    var aString: String = null
    var aEnum: TestEnum = null
    var aBinary: ByteBuffer = null
    var _passthroughFields: Builder[(Short, TFieldBlob), immutable$Map[Short, TFieldBlob]] = null
    var _done = false

    _iprot.readStructBegin()
    while (!_done) {
      val _field = _iprot.readFieldBegin()
      if (_field.`type` == TType.STOP) {
        _done = true
      } else {
        _field.id match {
          case 1 =>
            _field.`type` match {
              case TType.BOOL => {
                aBool = readABoolValue(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.BOOL

                throw new TProtocolException(
                  "Received wrong type for field 'aBool' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)))
            }
          case 2 =>
            _field.`type` match {
              case TType.BYTE => {
                aByte = readAByteValue(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.BYTE

                throw new TProtocolException(
                  "Received wrong type for field 'aByte' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)))
            }
          case 3 =>
            _field.`type` match {
              case TType.I16 => {
                aI16 = readAI16Value(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.I16

                throw new TProtocolException(
                  "Received wrong type for field 'aI16' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)))
            }
          case 4 =>
            _field.`type` match {
              case TType.I32 => {
                aI32 = readAI32Value(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.I32

                throw new TProtocolException(
                  "Received wrong type for field 'aI32' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)))
            }
          case 5 =>
            _field.`type` match {
              case TType.I64 => {
                aI64 = readAI64Value(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.I64

                throw new TProtocolException(
                  "Received wrong type for field 'aI64' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)))
            }
          case 6 =>
            _field.`type` match {
              case TType.DOUBLE => {
                aDouble = readADoubleValue(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.DOUBLE

                throw new TProtocolException(
                  "Received wrong type for field 'aDouble' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)))
            }
          case 7 =>
            _field.`type` match {
              case TType.STRING => {
                aString = readAStringValue(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.STRING

                throw new TProtocolException(
                  "Received wrong type for field 'aString' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)))
            }
          case 8 =>
            _field.`type` match {
              case TType.I32 | TType.ENUM => {
                aEnum = readAEnumValue(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.ENUM

                throw new TProtocolException(
                  "Received wrong type for field 'aEnum' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)))
            }
          case 9 =>
            _field.`type` match {
              case TType.STRING => {
                aBinary = readABinaryValue(_iprot)
              }
              case _actualType =>
                val _expectedType = TType.STRING

                throw new TProtocolException(
                  "Received wrong type for field 'aBinary' (expected=%s, actual=%s).".format(
                    ttypeToHuman(_expectedType),
                    ttypeToHuman(_actualType)))
            }
          case _ =>
            if (_passthroughFields == null)
              _passthroughFields = immutable$Map.newBuilder[Short, TFieldBlob]
            _passthroughFields += (_field.id -> TFieldBlob.read(_field, _iprot))
        }
        _iprot.readFieldEnd()
      }
    }
    _iprot.readStructEnd()

    new Immutable(
      aBool,
      aByte,
      aI16,
      aI32,
      aI64,
      aDouble,
      aString,
      aEnum,
      aBinary,
      if (_passthroughFields == null)
        NoPassthroughFields
      else
        _passthroughFields.result())
  }

  def apply(
    aBool: Boolean,
    aByte: Byte,
    aI16: Short,
    aI32: Int,
    aI64: Long,
    aDouble: Double,
    aString: String,
    aEnum: TestEnum,
    aBinary: ByteBuffer): TestTypes =
    new Immutable(
      aBool,
      aByte,
      aI16,
      aI32,
      aI64,
      aDouble,
      aString,
      aEnum,
      aBinary)

  def unapply(_item: TestTypes): Option[scala.Product9[Boolean, Byte, Short, Int, Long, Double, String, TestEnum, ByteBuffer]] = Some(_item)

  private def readABoolValue(_iprot: TProtocol): Boolean = {
    _iprot.readBool()
  }

  private def writeABoolField(aBool_item: Boolean, _oprot: TProtocol) {
    _oprot.writeFieldBegin(ABoolField)
    writeABoolValue(aBool_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeABoolValue(aBool_item: Boolean, _oprot: TProtocol) {
    _oprot.writeBool(aBool_item)
  }

  private def readAByteValue(_iprot: TProtocol): Byte = {
    _iprot.readByte()
  }

  private def writeAByteField(aByte_item: Byte, _oprot: TProtocol) {
    _oprot.writeFieldBegin(AByteField)
    writeAByteValue(aByte_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeAByteValue(aByte_item: Byte, _oprot: TProtocol) {
    _oprot.writeByte(aByte_item)
  }

  private def readAI16Value(_iprot: TProtocol): Short = {
    _iprot.readI16()
  }

  private def writeAI16Field(aI16_item: Short, _oprot: TProtocol) {
    _oprot.writeFieldBegin(AI16Field)
    writeAI16Value(aI16_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeAI16Value(aI16_item: Short, _oprot: TProtocol) {
    _oprot.writeI16(aI16_item)
  }

  private def readAI32Value(_iprot: TProtocol): Int = {
    _iprot.readI32()
  }

  private def writeAI32Field(aI32_item: Int, _oprot: TProtocol) {
    _oprot.writeFieldBegin(AI32Field)
    writeAI32Value(aI32_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeAI32Value(aI32_item: Int, _oprot: TProtocol) {
    _oprot.writeI32(aI32_item)
  }

  private def readAI64Value(_iprot: TProtocol): Long = {
    _iprot.readI64()
  }

  private def writeAI64Field(aI64_item: Long, _oprot: TProtocol) {
    _oprot.writeFieldBegin(AI64Field)
    writeAI64Value(aI64_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeAI64Value(aI64_item: Long, _oprot: TProtocol) {
    _oprot.writeI64(aI64_item)
  }

  private def readADoubleValue(_iprot: TProtocol): Double = {
    _iprot.readDouble()
  }

  private def writeADoubleField(aDouble_item: Double, _oprot: TProtocol) {
    _oprot.writeFieldBegin(ADoubleField)
    writeADoubleValue(aDouble_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeADoubleValue(aDouble_item: Double, _oprot: TProtocol) {
    _oprot.writeDouble(aDouble_item)
  }

  private def readAStringValue(_iprot: TProtocol): String = {
    _iprot.readString()
  }

  private def writeAStringField(aString_item: String, _oprot: TProtocol) {
    _oprot.writeFieldBegin(AStringField)
    writeAStringValue(aString_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeAStringValue(aString_item: String, _oprot: TProtocol) {
    _oprot.writeString(aString_item)
  }

  private def readAEnumValue(_iprot: TProtocol): TestEnum = {
    com.twitter.scalding.thrift.macros.scalathrift.TestEnum(_iprot.readI32())
  }

  private def writeAEnumField(aEnum_item: TestEnum, _oprot: TProtocol) {
    _oprot.writeFieldBegin(AEnumFieldI32)
    writeAEnumValue(aEnum_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeAEnumValue(aEnum_item: TestEnum, _oprot: TProtocol) {
    _oprot.writeI32(aEnum_item.value)
  }

  private def readABinaryValue(_iprot: TProtocol): ByteBuffer = {
    _iprot.readBinary()
  }

  private def writeABinaryField(aBinary_item: ByteBuffer, _oprot: TProtocol) {
    _oprot.writeFieldBegin(ABinaryField)
    writeABinaryValue(aBinary_item, _oprot)
    _oprot.writeFieldEnd()
  }

  private def writeABinaryValue(aBinary_item: ByteBuffer, _oprot: TProtocol) {
    _oprot.writeBinary(aBinary_item)
  }

  private def ttypeToHuman(byte: Byte) = {
    // from https://github.com/apache/thrift/blob/master/lib/java/src/org/apache/thrift/protocol/TType.java
    byte match {
      case TType.STOP => "STOP"
      case TType.VOID => "VOID"
      case TType.BOOL => "BOOL"
      case TType.BYTE => "BYTE"
      case TType.DOUBLE => "DOUBLE"
      case TType.I16 => "I16"
      case TType.I32 => "I32"
      case TType.I64 => "I64"
      case TType.STRING => "STRING"
      case TType.STRUCT => "STRUCT"
      case TType.MAP => "MAP"
      case TType.SET => "SET"
      case TType.LIST => "LIST"
      case TType.ENUM => "ENUM"
      case _ => "UNKNOWN"
    }
  }

  object Immutable extends ThriftStructCodec3[TestTypes] {
    override def encode(_item: TestTypes, _oproto: TProtocol) { _item.write(_oproto) }
    override def decode(_iprot: TProtocol): TestTypes = TestTypes.decode(_iprot)
  }

  /**
   * The default read-only implementation of TestTypes.  You typically should not need to
   * directly reference this class; instead, use the TestTypes.apply method to construct
   * new instances.
   */
  class Immutable(
    val aBool: Boolean,
    val aByte: Byte,
    val aI16: Short,
    val aI32: Int,
    val aI64: Long,
    val aDouble: Double,
    val aString: String,
    val aEnum: TestEnum,
    val aBinary: ByteBuffer,
    override val _passthroughFields: immutable$Map[Short, TFieldBlob]) extends TestTypes {
    def this(
      aBool: Boolean,
      aByte: Byte,
      aI16: Short,
      aI32: Int,
      aI64: Long,
      aDouble: Double,
      aString: String,
      aEnum: TestEnum,
      aBinary: ByteBuffer) = this(
      aBool,
      aByte,
      aI16,
      aI32,
      aI64,
      aDouble,
      aString,
      aEnum,
      aBinary,
      Map.empty)
  }

  /**
   * This Proxy trait allows you to extend the TestTypes trait with additional state or
   * behavior and implement the read-only methods from TestTypes using an underlying
   * instance.
   */
  trait Proxy extends TestTypes {
    protected def _underlying_TestTypes: TestTypes
    override def aBool: Boolean = _underlying_TestTypes.aBool
    override def aByte: Byte = _underlying_TestTypes.aByte
    override def aI16: Short = _underlying_TestTypes.aI16
    override def aI32: Int = _underlying_TestTypes.aI32
    override def aI64: Long = _underlying_TestTypes.aI64
    override def aDouble: Double = _underlying_TestTypes.aDouble
    override def aString: String = _underlying_TestTypes.aString
    override def aEnum: TestEnum = _underlying_TestTypes.aEnum
    override def aBinary: ByteBuffer = _underlying_TestTypes.aBinary
    override def _passthroughFields = _underlying_TestTypes._passthroughFields
  }
}

trait TestTypes
  extends ThriftStruct
  with scala.Product9[Boolean, Byte, Short, Int, Long, Double, String, TestEnum, ByteBuffer]
  with java.io.Serializable {
  import TestTypes._

  def aBool: Boolean
  def aByte: Byte
  def aI16: Short
  def aI32: Int
  def aI64: Long
  def aDouble: Double
  def aString: String
  def aEnum: TestEnum
  def aBinary: ByteBuffer

  def _passthroughFields: immutable$Map[Short, TFieldBlob] = immutable$Map.empty

  def _1 = aBool
  def _2 = aByte
  def _3 = aI16
  def _4 = aI32
  def _5 = aI64
  def _6 = aDouble
  def _7 = aString
  def _8 = aEnum
  def _9 = aBinary

  /**
   * Gets a field value encoded as a binary blob using TCompactProtocol.  If the specified field
   * is present in the passthrough map, that value is returend.  Otherwise, if the specified field
   * is known and not optional and set to None, then the field is serialized and returned.
   */
  def getFieldBlob(_fieldId: Short): Option[TFieldBlob] = {
    lazy val _buff = new TMemoryBuffer(32)
    lazy val _oprot = new TCompactProtocol(_buff)
    _passthroughFields.get(_fieldId) orElse {
      val _fieldOpt: Option[TField] =
        _fieldId match {
          case 1 =>
            if (true) {
              writeABoolValue(aBool, _oprot)
              Some(TestTypes.ABoolField)
            } else {
              None
            }
          case 2 =>
            if (true) {
              writeAByteValue(aByte, _oprot)
              Some(TestTypes.AByteField)
            } else {
              None
            }
          case 3 =>
            if (true) {
              writeAI16Value(aI16, _oprot)
              Some(TestTypes.AI16Field)
            } else {
              None
            }
          case 4 =>
            if (true) {
              writeAI32Value(aI32, _oprot)
              Some(TestTypes.AI32Field)
            } else {
              None
            }
          case 5 =>
            if (true) {
              writeAI64Value(aI64, _oprot)
              Some(TestTypes.AI64Field)
            } else {
              None
            }
          case 6 =>
            if (true) {
              writeADoubleValue(aDouble, _oprot)
              Some(TestTypes.ADoubleField)
            } else {
              None
            }
          case 7 =>
            if (aString ne null) {
              writeAStringValue(aString, _oprot)
              Some(TestTypes.AStringField)
            } else {
              None
            }
          case 8 =>
            if (aEnum ne null) {
              writeAEnumValue(aEnum, _oprot)
              Some(TestTypes.AEnumField)
            } else {
              None
            }
          case 9 =>
            if (aBinary ne null) {
              writeABinaryValue(aBinary, _oprot)
              Some(TestTypes.ABinaryField)
            } else {
              None
            }
          case _ => None
        }
      _fieldOpt match {
        case Some(_field) =>
          val _data = Arrays.copyOfRange(_buff.getArray, 0, _buff.length)
          Some(TFieldBlob(_field, _data))
        case None =>
          None
      }
    }
  }

  /**
   * Collects TCompactProtocol-encoded field values according to `getFieldBlob` into a map.
   */
  def getFieldBlobs(ids: TraversableOnce[Short]): immutable$Map[Short, TFieldBlob] =
    (ids flatMap { id => getFieldBlob(id) map { id -> _ } }).toMap

  /**
   * Sets a field using a TCompactProtocol-encoded binary blob.  If the field is a known
   * field, the blob is decoded and the field is set to the decoded value.  If the field
   * is unknown and passthrough fields are enabled, then the blob will be stored in
   * _passthroughFields.
   */
  def setField(_blob: TFieldBlob): TestTypes = {
    var aBool: Boolean = this.aBool
    var aByte: Byte = this.aByte
    var aI16: Short = this.aI16
    var aI32: Int = this.aI32
    var aI64: Long = this.aI64
    var aDouble: Double = this.aDouble
    var aString: String = this.aString
    var aEnum: TestEnum = this.aEnum
    var aBinary: ByteBuffer = this.aBinary
    var _passthroughFields = this._passthroughFields
    _blob.id match {
      case 1 =>
        aBool = readABoolValue(_blob.read)
      case 2 =>
        aByte = readAByteValue(_blob.read)
      case 3 =>
        aI16 = readAI16Value(_blob.read)
      case 4 =>
        aI32 = readAI32Value(_blob.read)
      case 5 =>
        aI64 = readAI64Value(_blob.read)
      case 6 =>
        aDouble = readADoubleValue(_blob.read)
      case 7 =>
        aString = readAStringValue(_blob.read)
      case 8 =>
        aEnum = readAEnumValue(_blob.read)
      case 9 =>
        aBinary = readABinaryValue(_blob.read)
      case _ => _passthroughFields += (_blob.id -> _blob)
    }
    new Immutable(
      aBool,
      aByte,
      aI16,
      aI32,
      aI64,
      aDouble,
      aString,
      aEnum,
      aBinary,
      _passthroughFields)
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is subtracked
   * from the passthroughFields map, if present.
   */
  def unsetField(_fieldId: Short): TestTypes = {
    var aBool: Boolean = this.aBool
    var aByte: Byte = this.aByte
    var aI16: Short = this.aI16
    var aI32: Int = this.aI32
    var aI64: Long = this.aI64
    var aDouble: Double = this.aDouble
    var aString: String = this.aString
    var aEnum: TestEnum = this.aEnum
    var aBinary: ByteBuffer = this.aBinary

    _fieldId match {
      case 1 =>
        aBool = false
      case 2 =>
        aByte = 0
      case 3 =>
        aI16 = 0
      case 4 =>
        aI32 = 0
      case 5 =>
        aI64 = 0L
      case 6 =>
        aDouble = 0.0
      case 7 =>
        aString = null
      case 8 =>
        aEnum = null
      case 9 =>
        aBinary = null
      case _ =>
    }
    new Immutable(
      aBool,
      aByte,
      aI16,
      aI32,
      aI64,
      aDouble,
      aString,
      aEnum,
      aBinary,
      _passthroughFields - _fieldId)
  }

  /**
   * If the specified field is optional, it is set to None.  Otherwise, if the field is
   * known, it is reverted to its default value; if the field is unknown, it is subtracked
   * from the passthroughFields map, if present.
   */
  def unsetABool: TestTypes = unsetField(1)

  def unsetAByte: TestTypes = unsetField(2)

  def unsetAI16: TestTypes = unsetField(3)

  def unsetAI32: TestTypes = unsetField(4)

  def unsetAI64: TestTypes = unsetField(5)

  def unsetADouble: TestTypes = unsetField(6)

  def unsetAString: TestTypes = unsetField(7)

  def unsetAEnum: TestTypes = unsetField(8)

  def unsetABinary: TestTypes = unsetField(9)

  override def write(_oprot: TProtocol) {
    TestTypes.validate(this)
    _oprot.writeStructBegin(Struct)
    writeABoolField(aBool, _oprot)
    writeAByteField(aByte, _oprot)
    writeAI16Field(aI16, _oprot)
    writeAI32Field(aI32, _oprot)
    writeAI64Field(aI64, _oprot)
    writeADoubleField(aDouble, _oprot)
    if (aString ne null) writeAStringField(aString, _oprot)
    if (aEnum ne null) writeAEnumField(aEnum, _oprot)
    if (aBinary ne null) writeABinaryField(aBinary, _oprot)
    _passthroughFields.values foreach { _.write(_oprot) }
    _oprot.writeFieldStop()
    _oprot.writeStructEnd()
  }

  def copy(
    aBool: Boolean = this.aBool,
    aByte: Byte = this.aByte,
    aI16: Short = this.aI16,
    aI32: Int = this.aI32,
    aI64: Long = this.aI64,
    aDouble: Double = this.aDouble,
    aString: String = this.aString,
    aEnum: TestEnum = this.aEnum,
    aBinary: ByteBuffer = this.aBinary,
    _passthroughFields: immutable$Map[Short, TFieldBlob] = this._passthroughFields): TestTypes =
    new Immutable(
      aBool,
      aByte,
      aI16,
      aI32,
      aI64,
      aDouble,
      aString,
      aEnum,
      aBinary,
      _passthroughFields)

  override def canEqual(other: Any): Boolean = other.isInstanceOf[TestTypes]

  override def equals(other: Any): Boolean =
    _root_.scala.runtime.ScalaRunTime._equals(this, other) &&
      _passthroughFields == other.asInstanceOf[TestTypes]._passthroughFields

  override def hashCode: Int = _root_.scala.runtime.ScalaRunTime._hashCode(this)

  override def toString: String = _root_.scala.runtime.ScalaRunTime._toString(this)

  override def productArity: Int = 9

  override def productElement(n: Int): Any = n match {
    case 0 => this.aBool
    case 1 => this.aByte
    case 2 => this.aI16
    case 3 => this.aI32
    case 4 => this.aI64
    case 5 => this.aDouble
    case 6 => this.aString
    case 7 => this.aEnum
    case 8 => this.aBinary
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productPrefix: String = "TestTypes"
}