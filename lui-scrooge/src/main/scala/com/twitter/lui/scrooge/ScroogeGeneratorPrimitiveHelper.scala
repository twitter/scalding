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

object ScroogeGeneratorPrimitiveHelper {
  import ScroogeGenerator._

  def nonMethod(f: ThriftStructFieldInfo, existingPath: ExistingPath,
    cols: Array[BaseColumnReader],
    mapData: Map[String, (ParquetFieldInfo, Option[Int], Option[ParquetThriftEnumInfo], Vector[Short])],
    fieldIdx: Int): (String, String) = {

    val path = if (existingPath.toStr.isEmpty) f.id.toString else s"${existingPath.toStr}/${f.id}"
    val primitiveZero = bytePrimtiveZero(f)
    val getter = byteToJavaGetter(f)

    mapData.get(path).map {
      case (parquetFieldInfo, Some(colIdx), enumInfoOpti, _) =>
        val setString = if (f.isOptional)
          s"""if(isNull) {
              None
            } else {
              Some($getter)
            }
            """
        else
          s"if(isNull) $primitiveZero else $getter"

        ("", s"""{
              val rdr = colReaders($colIdx)
              val isNull: Boolean = !rdr.primitiveAdvance(recordIdx, repetitionOffsets)
              $setString
            }
          """)
      case args @ _ => sys.error(s"Invalid args: $args")
    }.getOrElse("", if (f.isOptional) "None" else primitiveZero)
  }

  def apply(f: ThriftStructFieldInfo, existingPath: ExistingPath,
    cols: Array[BaseColumnReader],
    mapData: Map[String, (ParquetFieldInfo, Option[Int], Option[ParquetThriftEnumInfo], Vector[Short])],
    fieldIdx: Int): (String, String) = {
    val tpeString = byteToJavaPrimitive(f)

    val (objectSetup, innerBuilder) = nonMethod(f, existingPath, cols, mapData, fieldIdx)

    ("", s"""
          lazy val `${f.name}`: $tpeString = $innerBuilder
          """)
  }
}
