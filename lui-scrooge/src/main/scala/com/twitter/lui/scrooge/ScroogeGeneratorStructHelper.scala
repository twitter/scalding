package com.twitter.lui.scrooge

import com.twitter.algebird.Monoid
import com.twitter.lui.column_reader.BaseColumnReader
import com.twitter.lui.column_reader.cache.CacheColumnReader
import com.twitter.lui.column_reader.noncache.NonCacheColumnReader
import com.twitter.scrooge.ThriftEnum
import com.twitter.scrooge.{ ThriftStruct, ThriftUnion, ThriftStructField }
import com.twitter.scrooge.{ ThriftStructCodec3, ThriftStructFieldInfo }
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.schema.{ MessageType, Type => ParquetSchemaType }
import org.apache.parquet.thrift.ThriftMetaData
import org.apache.thrift.protocol.TType
import scala.collection.JavaConverters._
import scala.collection.mutable.{ Map => MMap }
import scala.reflect.ClassTag

object ScroogeGeneratorStructHelper {
  import ScroogeGenerator._

  def nonMethod(f: ThriftStructFieldInfo, existingPath: ExistingPath,
    cols: Array[BaseColumnReader],
    mapData: Map[String, (ParquetFieldInfo, Option[Int], Option[ParquetThriftEnumInfo], Vector[Short])],
    fieldIdx: Int): (String, String) = {
    val path = if (existingPath.toStr.isEmpty) f.id.toString else s"${existingPath.toStr}/${f.id}"
    val tpeString = byteToJavaPrimitive(f) // Can include things like Option[String]
    val tpeStringNoOption = byteToJavaPrimitive(f, false) // in the example above would be String instead
    val nullV = if (f.isOptional) "None" else "null"

    mapData.get(path).map{
      case (locFieldInfo, _, _, pathV) =>
        val fieldRepetitionLevel = locFieldInfo.maxRepetitionLevel
        val fieldDefinitionLevel = locFieldInfo.maxDefinitionLevel
        val (_, Some(fieldClosestPrimitiveColumnIdx), _, _) = mapData(locFieldInfo.closestPrimitiveChild.get.toVector.mkString("/"))

        val buildType = if (classOf[ThriftUnion].isAssignableFrom(Class.forName(tpeStringNoOption))) "Union" else "Struct"

        val innerBuilder = s"""scroogeGenerator.build${buildType}(classOf[$tpeStringNoOption], $tpeStringNoOption.fieldInfos , "${pathV.mkString("/")}", colReaders, recordIdx, repetitionOffsets).asInstanceOf[$tpeStringNoOption]"""

        val someV = if (f.isOptional)
          s"Some($innerBuilder)"
        else
          innerBuilder

        val notNullCall = if (fieldRepetitionLevel > 0)
          s"""notNull($fieldDefinitionLevel,
              recordIdx,
              Array[Int](0),
              Array[Int](${(0 until fieldRepetitionLevel + 2).map(_ => 0).mkString(",")}),
              repetitionOffsets)"""
        else
          s"notNull($fieldDefinitionLevel, recordIdx)"

        ("", s"""{
          val primitive = colReaders($fieldClosestPrimitiveColumnIdx).asInstanceOf[CacheColumnReader]
          primitive.advanceSetRecord(recordIdx)

          if(primitive.$notNullCall) {
              $someV
            } else {
              $nullV
            }
          }
          """)
    }.getOrElse {
      ("", s"$nullV")
    }
  }

  def apply(f: ThriftStructFieldInfo, existingPath: ExistingPath,
    cols: Array[BaseColumnReader],
    mapData: Map[String, (ParquetFieldInfo, Option[Int], Option[ParquetThriftEnumInfo], Vector[Short])],
    fieldIdx: Int): (String, String) = {

    val (objectCode, innerGetCode) = nonMethod(f, existingPath, cols, mapData, fieldIdx)
    val tpeString = byteToJavaPrimitive(f) // Can include things like Option[String]

    (objectCode, s"""
          lazy val ${f.name}: $tpeString = $innerGetCode
          """)
  }
}
