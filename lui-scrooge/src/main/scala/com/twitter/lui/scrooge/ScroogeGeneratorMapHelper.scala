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

object ScroogeGeneratorMapHelper {
  import ScroogeGenerator._
  def clazzToTpeAndColumn(clazzF: Class[_], colName: String, fieldIdx: Int, innerIdx: Int, path: Vector[Short]) =
    clazzF.getName match {
      case "java.lang.String" => ("String", raw"{$colName.primitiveAdvance(recordIdx, repetitionOffsets); $colName.getString()}", true)
      case "boolean" => ("Boolean", raw"{$colName.primitiveAdvance(recordIdx, repetitionOffsets); $colName.getBoolean()}", true)
      case "byte" => ("Byte", raw"{$colName.primitiveAdvance(recordIdx, repetitionOffsets); $colName.getInteger().toByte}", true)
      case "double" => ("Double", raw"{$colName.primitiveAdvance(recordIdx, repetitionOffsets); $colName.getDouble()}", true)
      case "int" => ("Int", raw"{$colName.primitiveAdvance(recordIdx, repetitionOffsets); $colName.getInteger()}", true)
      case "long" => ("Long", raw"{$colName.primitiveAdvance(recordIdx, repetitionOffsets); $colName.getLong()}", true)
      case "short" => ("Short", raw"{$colName.primitiveAdvance(recordIdx, repetitionOffsets); $colName.getInteger().toShort}", true)
      case name if classOf[ThriftEnum].isAssignableFrom(clazzF) =>
        (name, raw"""{$colName.primitiveAdvance(recordIdx, repetitionOffsets); enumDecode${fieldIdx}_${innerIdx}($colName.getString())}""", true)
      case name if classOf[ThriftUnion].isAssignableFrom(clazzF) =>
        (name, raw"""scroogeGenerator.buildStruct(classOf[$name], $name.fieldInfos, "${path.mkString("/")}", colReaders, recordIdx, repetitionOffsets).asInstanceOf[$name]""", false)
      case name if classOf[ThriftStruct].isAssignableFrom(clazzF) =>
        (name, raw"""scroogeGenerator.buildStruct(classOf[$name], $name.fieldInfos, "${path.mkString("/")}", colReaders, recordIdx, repetitionOffsets).asInstanceOf[$name]""", false)
    }

  def apply(f: ThriftStructFieldInfo, existingPath: ExistingPath,
    cols: Array[BaseColumnReader],
    mapData: Map[String, (ParquetFieldInfo, Option[Int], Option[ParquetThriftEnumInfo], Vector[Short])],
    fieldIdx: Int): (String, String) = {

    val path = if (existingPath.toStr.isEmpty) f.id.toString else s"${existingPath.toStr}/${f.id}"

    val (keyManifest, valueManifest) = (f.manifest.typeArguments(0), f.manifest.typeArguments(1))
    val (keyClazz, valueClazz) = (keyManifest.runtimeClass, valueManifest.runtimeClass)

    val (locFieldInfo, _, _, _) = mapData(path)

    val (keyFieldInfo, keyColumnOpt, _, pathK) = mapData(s"$path/1")
    val (valueFieldInfo, valueColumnOpt, _, pathV) = mapData(s"$path/2")

    val fieldRepetitionLevel = keyFieldInfo.maxRepetitionLevel
    val fieldDefinitionLevel = keyFieldInfo.maxDefinitionLevel

    // Handling the case here of a list of primitives,
    // there is no sub child to ask about a closest primitive
    val (_, Some(fieldClosestPrimitiveColumnIdx), _, _) = locFieldInfo.closestPrimitiveChild.map { pri => mapData(pri.toVector.mkString("/")) }.getOrElse(mapData(path))

    val (keySubTpeName, keyReadColumn, keyIsPrimitive) = clazzToTpeAndColumn(keyClazz, "keyColumn", fieldIdx, 0, pathK)
    val (valueSubTpeName, valueReadColumn, valueIsPrimitive) = clazzToTpeAndColumn(valueClazz, "valueColumn", fieldIdx, 1, pathV)

    val fieldTypeString = if (f.isOptional) s"Option[Map[$keySubTpeName, $valueSubTpeName]]" else s"Map[$keySubTpeName, $valueSubTpeName]"

    // Here we need to handle the difference between None list and a Some(List())
    // we know the definition level for an optional for if the list is some or none will be up one level
    val setStatement = if (f.isOptional) s"""if(elements > 0) Some(mapBuilder.result()) else {
              if(primitive.notNull(${fieldDefinitionLevel - 1}, recordIdx,cachePos,stateOffests,repetitionOffsets)) Some(Map()) else None
              }"""
    else s"mapBuilder.result()"

    val setupStr = (keyIsPrimitive, valueIsPrimitive) match {
      case (true, true) =>
        s"""
                val keyColumn = colReaders(${keyColumnOpt.get}).asInstanceOf[CacheColumnReader]
                val valueColumn = colReaders(${valueColumnOpt.get}).asInstanceOf[CacheColumnReader]
                val primitive = keyColumn
                keyColumn.advanceSetRecord(recordIdx)
                valueColumn.advanceSetRecord(recordIdx)
              """
      case (true, false) =>
        s"""
                val keyColumn = colReaders(${keyColumnOpt.get}).asInstanceOf[CacheColumnReader]
                val primitive = keyColumn
                keyColumn.advanceSetRecord(recordIdx)
              """

      case (false, true) =>
        s"""
                val primitive = colReaders($fieldClosestPrimitiveColumnIdx).asInstanceOf[CacheColumnReader]
                val valueColumn = colReaders(${valueColumnOpt.get}).asInstanceOf[CacheColumnReader]
                primitive.advanceSetRecord(recordIdx)
                valueColumn.advanceSetRecord(recordIdx)
              """

      case (false, false) =>
        s"""
                val primitive = colReaders($fieldClosestPrimitiveColumnIdx).asInstanceOf[CacheColumnReader]
                primitive.advanceSetRecord(recordIdx)
              """
    }

    ("", raw"""
          lazy val `${f.name}`: $fieldTypeString = {
              $setupStr
              val mapBuilder = Map.newBuilder[$keySubTpeName, $valueSubTpeName]
              val cachePos = Array[Int](0)
              val stateOffests = Array[Int](${(0 until fieldRepetitionLevel + 2).map(_ => 0).mkString(",")})
              var elements: Int = 0

              while(primitive.notNull($fieldDefinitionLevel, recordIdx,cachePos,stateOffests,repetitionOffsets)) {
                val a = ($keyReadColumn, $valueReadColumn)
                mapBuilder += a
                repetitionOffsets($fieldRepetitionLevel) += 1
                elements += 1
              }
              repetitionOffsets($fieldRepetitionLevel) = 0

              $setStatement
            }

          """)
  }
}
