package com.twitter.lui.scrooge

import com.twitter.algebird.Monoid
import com.twitter.lui.column_reader.BaseColumnReader
import com.twitter.lui.column_reader.cache.CacheColumnReader
import com.twitter.lui.column_reader.noncache.NonCacheColumnReader
import com.twitter.scrooge.ThriftEnum
import com.twitter.scrooge.{ ThriftStruct, ThriftUnion, ThriftStructField }
import com.twitter.scrooge.{ ThriftStructCodec3, ThriftStructFieldInfo, ThriftUnionFieldInfo }
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.schema.{ MessageType, Type => ParquetSchemaType }
import org.apache.parquet.thrift.ThriftMetaData
import org.apache.thrift.protocol.TType
import scala.collection.JavaConverters._
import scala.collection.mutable.{ Map => MMap }
import scala.reflect.ClassTag

case class ExistingPath(toStr: String) extends AnyVal

case class CustomCompiler() {
  import scala.reflect.runtime.universe._
  import scala.tools.reflect.ToolBox
  private[this] val toolbox = runtimeMirror(getClass.getClassLoader).mkToolBox()

  def compile[T](code: String): T = {
    val startTime = System.nanoTime()
    val compiledCode = toolbox.compile(toolbox.parse(code))

    val endTime = System.nanoTime()
    def timeMs: Double = (endTime - startTime).toDouble / 1000000
    println(s"Compiled Java code (${code.size} bytes) in $timeMs ms")
    compiledCode().asInstanceOf[T]
  }

}

object ScroogeGenerator {
  sealed trait GenTypes
  case object StructT extends GenTypes
  case object PrimitiveT extends GenTypes
  case object ListT extends GenTypes
  case object SetT extends GenTypes
  case object MapT extends GenTypes

  def byteToJavaPrimitive(f: ThriftStructFieldInfo, allowOption: Boolean = true): String = {
    if (f.isOptional && allowOption) s"Option[${byteToJavaPrimitive(f, false)}]" else
      f.tpe match {
        case TType.BOOL => "Boolean"
        case TType.BYTE => "Byte"
        case TType.DOUBLE => "Double"
        case TType.I16 => "Short"
        case TType.I32 => "Int"
        case TType.I64 => "Long"
        case TType.STRING => f.manifest.runtimeClass.getName
        case TType.STRUCT => f.manifest.runtimeClass.getName
        case TType.MAP => f.manifest.runtimeClass.getName
        case TType.LIST => f.manifest.runtimeClass.getName
        case TType.SET => f.manifest.runtimeClass.getName
        case TType.ENUM => f.manifest.runtimeClass.getName
      }
  }

  def byteToJavaGetter(f: ThriftStructFieldInfo): String =
    f.tpe match {
      case 2 => "rdr.getBoolean()"
      case 3 => "rdr.getInteger().toByte"
      case 4 => "rdr.getDouble()"
      case 6 => "rdr.getInteger().toShort"
      case 8 => "rdr.getInteger()"
      case 10 => "rdr.getLong()"
      case 11 if (f.manifest.runtimeClass.getName == "java.nio.ByteBuffer") => "rdr.getBinary()"
      case 11 => "rdr.getString()"
      case 16 => s"${f.name}enumDecode(rdr.getString())"
      case _ => """sys.error("unreachable")"""
    }

  def bytePrimtiveZero(f: ThriftStructFieldInfo): String = {
    if (f.isOptional) s"None" else
      f.tpe match {
        case TType.BOOL => "false"
        case TType.BYTE => "0"
        case TType.DOUBLE => "0.0"
        case TType.I16 => "0"
        case TType.I32 => "0"
        case TType.I64 => "0L"
        case TType.STRING => "null"
        case TType.ENUM => "null"
        case _ => """sys.error("unreachable")"""
      }
  }

  implicit class ThriftStructFieldInfoExtensions(val tsfie: ThriftStructFieldInfo) extends AnyVal {
    def tpe: Byte = tsfie.tfield.`type`
    def id: Short = tsfie.tfield.id

    def isStruct: Boolean = tpe == 12
    def toGenT = tpe match {
      case 2 => PrimitiveT
      case 3 => PrimitiveT
      case 4 => PrimitiveT
      case 6 => PrimitiveT
      case 8 => PrimitiveT
      case 10 => PrimitiveT
      case TType.STRING => PrimitiveT
      case TType.STRUCT => StructT
      case TType.LIST => ListT
      case TType.SET => SetT
      case TType.MAP => MapT
      case TType.ENUM => PrimitiveT
    }

    def name: String = {
      val str = tsfie.tfield.name
      str.takeWhile(_ == '_') + str.
        split('_').
        filterNot(_.isEmpty).
        zipWithIndex.map {
          case (part, ind) =>
            val first = if (ind == 0) part.charAt(0).toLower else part.charAt(0).toUpper
            val isAllUpperCase = part.forall(_.isUpper)
            val rest = if (isAllUpperCase) part.drop(1).toLowerCase else part.drop(1)
            new StringBuilder(part.length).append(first).append(rest)
        }.mkString
    }
  }
}

case class ScroogeGenerator(mapData: Map[String, (ParquetFieldInfo, Option[Int], Option[ParquetThriftEnumInfo], Vector[Short])]) {
  import ScroogeGenerator._
  private[this] val cache = MMap[String, (Array[BaseColumnReader], ScroogeGenerator, Long, Array[Int]) => Any]()
  // cache ++= Builders.prebuiltBuilders
  private[this] implicit val compiler = CustomCompiler()

  def buildBuilder[T <: ThriftStruct](className: String,
    existingPath: String,
    data: Array[BaseColumnReader],
    repetitionOffsets: Array[Int]): Long => T = {
    val mainClass = Class.forName(className).asInstanceOf[Class[T]]
    val companionClass = Class.forName(className + "$") // Get the module

    if (classOf[ThriftUnion].isAssignableFrom(mainClass)) {
      val fieldInfosMethod = companionClass.getMethod("fieldInfos")
      val companionInstance = companionClass.getField("MODULE$").get(null)
      val metadata: List[ThriftUnionFieldInfo[_, _]] = fieldInfosMethod.invoke(companionInstance).asInstanceOf[List[ThriftUnionFieldInfo[_, _]]]
      buildUnion[T](mainClass,
        metadata,
        existingPath,
        data,
        _,
        repetitionOffsets)
    } else { // ThriftStruct
      val fieldInfosMethod = companionClass.getMethod("fieldInfos")
      val companionInstance = companionClass.getField("MODULE$").get(null)
      val metadata: List[ThriftStructFieldInfo] = fieldInfosMethod.invoke(companionInstance).asInstanceOf[List[ThriftStructFieldInfo]]
      buildStruct[T](mainClass,
        metadata,
        existingPath,
        data,
        _,
        repetitionOffsets)
    }

  }

  def buildUnion[T <: ThriftStruct](mainClass: Class[T],
    metadata: List[ThriftUnionFieldInfo[_, _]],
    existingPath: String,
    data: Array[BaseColumnReader],
    recordIdx: java.lang.Long,
    repetitionOffsets: Array[Int]): T = {

    val builder = cache.getOrElseUpdate(existingPath,
      ScroogeGeneratorUnionBuilder[T](mapData, mainClass, metadata, ExistingPath(existingPath), data)).asInstanceOf[((Array[BaseColumnReader], ScroogeGenerator, Long, Array[Int]) => T)]
    builder(data, this, recordIdx, repetitionOffsets)

  }

  def buildStruct[T <: ThriftStruct](mainClass: Class[T],
    metadata: List[ThriftStructFieldInfo],
    existingPath: String,
    data: Array[BaseColumnReader],
    recordIdx: java.lang.Long,
    repetitionOffsets: Array[Int]): T = {
    val builder = cache.getOrElseUpdate(existingPath, ScroogeGeneratorStructBuilder[T](mapData, mainClass, metadata, ExistingPath(existingPath), data)).asInstanceOf[((Array[BaseColumnReader], ScroogeGenerator, Long, Array[Int]) => T)]
    builder(data, this, recordIdx, repetitionOffsets)
  }

}

