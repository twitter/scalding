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

object ScroogeGeneratorEnumHelper {
  import ScroogeGenerator._

  def apply(
    metadata: List[ThriftStructFieldInfo],
    existingPath: ExistingPath,
    cols: Array[BaseColumnReader],
    mapData: Map[String, (ParquetFieldInfo, Option[Int], Option[ParquetThriftEnumInfo], Vector[Short])]): String = {

    val enumFunctions = metadata.flatMap {
      f: ThriftStructFieldInfo =>
        val tpeStrNoOption = byteToJavaPrimitive(f, false)
        val path = if (existingPath.toStr.isEmpty) f.id.toString else s"${existingPath.toStr}/${f.id}"
        mapData.get(path).flatMap {
          case (_, _, enumInfoOpti, _) =>
            enumInfoOpti.map { enumData: ParquetThriftEnumInfo =>

              val mapStr = enumData.m.map {
                case (strV: String, thriftId: Int) =>
                  s""" "$strV" -> $thriftId """
              }.mkString(",")
              raw"""
          private[this] val ${f.name}EnumDataMap = Map($mapStr)
          @inline
          private def ${f.name}enumDecode(strV: String): $tpeStrNoOption =
            $tpeStrNoOption.getOrUnknown(${f.name}EnumDataMap(strV))
            """
            }
        }

    }

    val enumFunctionsFromMaps = metadata.zipWithIndex.flatMap {
      case (f: ThriftStructFieldInfo, fIndx) =>
        f.toGenT match {
          case MapT =>
            val (keyManifest, valueManifest) = (f.manifest.typeArguments(0), f.manifest.typeArguments(1))
            val (keyClazz, valueClazz) = (keyManifest.runtimeClass, valueManifest.runtimeClass)
            List(keyClazz, valueClazz).zipWithIndex.filter(tup => classOf[ThriftEnum].isAssignableFrom(tup._1)).map {
              case (clazz, idx) =>
                val path = if (existingPath.toStr.isEmpty) s"${f.id.toString}/${idx + 1}" else s"${existingPath.toStr}/${f.id}/${idx + 1}"
                val (_, _, enumInfoOpti, _) = mapData.get(path).getOrElse {
                  mapData.keys.foreach(println)
                  sys.error(s"Could not find $path in map")
                }
                val enumData = enumInfoOpti.get // must be present or error
                val mapStr = enumData.m.map {
                  case (strV: String, thriftId: Int) =>
                    s""" "$strV" -> $thriftId """
                }.mkString(",")

                s"""
          |val enumDataMap${fIndx}_${idx} = Map($mapStr)
          |@inline
          |private def enumDecode${fIndx}_${idx}(strV: String): ${clazz.getName} =
          |  ${clazz.getName}.getOrUnknown(enumDataMap${fIndx}_${idx}(strV))
          |""".stripMargin('|')
            }
          case _ => Nil
        }
    }

    (enumFunctionsFromMaps ++ enumFunctions).mkString("\n")
  }
}
