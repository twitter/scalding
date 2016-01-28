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

object ScroogeGeneratorListSetHelper {
  import ScroogeGenerator._

  def nonMethod(seqManifest: Manifest[_],
    existingPath: ExistingPath,
    cols: Array[BaseColumnReader],
    mapData: Map[String, (ParquetFieldInfo, Option[Int], Option[ParquetThriftEnumInfo], Vector[Short])],
    isOptional: Boolean,
    isRequired: Boolean): (String, String) = {

    val returnType = seqManifest.runtimeClass.getName match {
      case "scala.collection.Set" => "Set"
      case "scala.collection.Seq" => "Vector"
      case t => sys.error(s"Should not enter the List and Set helper with type $t")
    }

    val path = existingPath.toStr

    val manifestF = seqManifest.typeArguments.head
    val clazzF = manifestF.runtimeClass
    val (optionalGroupFieldInfo, _, _, _) = mapData(path)
    val (repeatedGroupFieldInfo, _, _, repeatedGroupPathV) = mapData(s"$path/1")
    val fieldRepetitionLevel = repeatedGroupFieldInfo.maxRepetitionLevel
    val fieldDefinitionLevel = repeatedGroupFieldInfo.maxDefinitionLevel

    // Handling the case here of a list of primitives,
    // there is no sub child to ask about a closest primitive
    val (_, Some(fieldClosestPrimitiveColumnIdx), _, _) = repeatedGroupFieldInfo.closestPrimitiveChild.map { pri => mapData(pri.toVector.mkString("/")) }.getOrElse(mapData(s"$path/1"))

    val (subTpeName, readColumn) = clazzF.getName match {
      case "java.nio.ByteBuffer" => ("java.nio.ByteBuffer", "{primitive.primitiveAdvance(recordIdx, repetitionOffsets); primitive.getBinary()}")
      case "java.lang.String" => ("String", "{primitive.primitiveAdvance(recordIdx, repetitionOffsets); primitive.getString()}")
      case "boolean" => ("Boolean", "{primitive.primitiveAdvance(recordIdx, repetitionOffsets); primitive.getBoolean()}")
      case "byte" => ("Byte", "{primitive.primitiveAdvance(recordIdx, repetitionOffsets); primitive.getInteger().toByte}")
      case "double" => ("Double", "{primitive.primitiveAdvance(recordIdx, repetitionOffsets); primitive.getDouble()}")
      case "int" => ("Int", "{primitive.primitiveAdvance(recordIdx, repetitionOffsets); primitive.getInteger()}")
      case "long" => ("Long", "{primitive.primitiveAdvance(recordIdx, repetitionOffsets); primitive.getLong()}")
      case "short" => ("Short", "{primitive.primitiveAdvance(recordIdx, repetitionOffsets); primitive.getInteger().toShort}")
      case "scala.collection.Seq" =>
        val innerClassName = manifestF.typeArguments.head.runtimeClass.getName
        (s"scala.collection.Seq[$innerClassName]", ScroogeGeneratorListSetHelper.nonMethod(manifestF, ExistingPath(repeatedGroupPathV.toVector.mkString("/")), cols, mapData, false, true)._2.split("\n").map(l => "    " + l).mkString("\n"))
      case name if classOf[ThriftEnum].isAssignableFrom(clazzF) => (name, s"""{primitive.primitiveAdvance(recordIdx, repetitionOffsets); ${name}enumDecode(primitive.getString())}""")
      case name if classOf[ThriftUnion].isAssignableFrom(clazzF) => (name, s"""scroogeGenerator.buildUnion(classOf[$name], $name.fieldInfos, "${repeatedGroupPathV.toVector.mkString("/")}", colReaders, recordIdx, repetitionOffsets).asInstanceOf[$name]""")
      case name if classOf[ThriftStruct].isAssignableFrom(clazzF) => (name, s"""scroogeGenerator.buildStruct(classOf[$name], $name.fieldInfos, "${repeatedGroupPathV.toVector.mkString("/")}", colReaders, recordIdx, repetitionOffsets).asInstanceOf[$name]""")
    }

    // Here we need to handle the difference between None list and a Some(List())
    // we know the definition level for an optional for if the list is some or none will be up one level
    val setStatement = (isOptional, isRequired) match {
      case (true, _) =>
        s"""
if(elements > 0)
  Some(listBuilder.result())
else {
  if(primitive.notNull(${optionalGroupFieldInfo.maxDefinitionLevel}, recordIdx,cachePos,stateOffests,repetitionOffsets))
    Some(${returnType}())
  else
    None
}"""
      case (_, false) => s"""
if(elements > 0)
  listBuilder.result()
  else {
    if(primitive.notNull(${optionalGroupFieldInfo.maxDefinitionLevel}, recordIdx,cachePos,stateOffests,repetitionOffsets))
      ${returnType}()
    else
      null
}"""
      case (_, true) => s"listBuilder.result()"
    }

    val maxRepLevelOfPrimitive = cols(fieldClosestPrimitiveColumnIdx).maxRepetitionLevel

    ("", raw"""{
              val primitive = colReaders($fieldClosestPrimitiveColumnIdx).asInstanceOf[CacheColumnReader]
              primitive.advanceSetRecord(recordIdx)
              val listBuilder = ${returnType}.newBuilder[$subTpeName]
              val cachePos = Array[Int](0)
              val stateOffests = Array[Int](${(0 until maxRepLevelOfPrimitive + 1).map(_ => 0).mkString(",")})
              var elements: Int = 0

              while(primitive.notNull($fieldDefinitionLevel, recordIdx,cachePos,stateOffests,repetitionOffsets)) {
                val a = $readColumn
                listBuilder += a
                repetitionOffsets($fieldRepetitionLevel) += 1
                elements += 1
              }
              repetitionOffsets($fieldRepetitionLevel) = 0

              $setStatement
            }
          """)
  }

  def apply(f: ThriftStructFieldInfo, existingPath: ExistingPath,
    cols: Array[BaseColumnReader],
    mapData: Map[String, (ParquetFieldInfo, Option[Int], Option[ParquetThriftEnumInfo], Vector[Short])],
    fieldIdx: Int): (String, String) = {

    val path = ExistingPath(if (existingPath.toStr.isEmpty) f.id.toString else s"${existingPath.toStr}/${f.id}")

    val (objectSettings, methodContents) = nonMethod(f.manifest, path, cols, mapData, f.isOptional, f.isRequired)

    val returnType = f.toGenT match {
      case SetT => "Set"
      case ListT => "Vector"
      case t => sys.error(s"Should not enter the List and Set helper with type $t")
    }
    val manifestF = f.manifest.typeArguments.head
    val clazzF = manifestF.runtimeClass
    val subTpeName = clazzF.getName match {
      case "boolean" => "Boolean"
      case "byte" => "Byte"
      case "double" => "Double"
      case "int" => "Int"
      case "long" => "Long"
      case "short" => "Short"
      case "scala.collection.Seq" =>
        val innerClassName = manifestF.typeArguments.head.runtimeClass.getName
        s"scala.collection.Seq[$innerClassName]"
      case rest => rest
    }

    val fieldTypeString = if (f.isOptional) s"Option[${returnType}[$subTpeName]]" else s"${returnType}[$subTpeName]"
    (objectSettings, raw"""lazy val `${f.name}`: $fieldTypeString = $methodContents""")
  }

}
