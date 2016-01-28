package com.twitter.lui.scrooge

import com.twitter.algebird.Monoid
import com.twitter.lui.column_reader.BaseColumnReader
import com.twitter.lui.column_reader.cache.CacheColumnReader
import com.twitter.lui.column_reader.noncache.NonCacheColumnReader
import com.twitter.scrooge.ThriftEnum
import com.twitter.scrooge.{ ThriftStruct, ThriftUnion, ThriftStructField }
import com.twitter.scrooge.{ ThriftStructCodec3, ThriftUnionFieldInfo, ThriftStructFieldInfo }
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.schema.{ MessageType, Type => ParquetSchemaType }
import org.apache.parquet.thrift.ThriftMetaData
import org.apache.thrift.protocol.TType
import scala.collection.JavaConverters._
import scala.collection.mutable.{ Map => MMap }
import scala.reflect.ClassTag

object ScroogeGeneratorUnionBuilder {
  import ScroogeGenerator._

  private[scrooge] def apply[T <: ThriftStruct](mapData: Map[String, (ParquetFieldInfo, Option[Int], Option[ParquetThriftEnumInfo], Vector[Short])],
    mainClass: Class[T],
    metadata: List[ThriftUnionFieldInfo[_, _]],
    existingPath: ExistingPath,
    cols: Array[BaseColumnReader])(implicit compiler: CustomCompiler): ((Array[BaseColumnReader], ScroogeGenerator, Long, Array[Int]) => T) = {

    val className = mainClass.getName
    val structFieldInfo = mapData(existingPath.toStr)._1

    val (objectSections: String, fieldSections: String) = Monoid.sum(metadata.zipWithIndex.map {
      case (structUnionFieldInfo: ThriftUnionFieldInfo[_, _], idx) =>
        val structFieldInfo = structUnionFieldInfo.structFieldInfo

        val (objectCode: String, builderStr: String) = structFieldInfo.toGenT match {
          case PrimitiveT => ScroogeGeneratorPrimitiveHelper.nonMethod(structFieldInfo, existingPath, cols, mapData, idx)

          case StructT => ScroogeGeneratorStructHelper.nonMethod(structFieldInfo, existingPath, cols, mapData, idx)

          // case ListT => ScroogeGeneratorListSetHelper(structFieldInfo, existingPath, cols, mapData, idx)

          // case SetT => ScroogeGeneratorListSetHelper(structFieldInfo, existingPath, cols, mapData, idx)

          // case MapT => ScroogeGeneratorMapHelper(structFieldInfo, existingPath, cols, mapData, idx)
        }

        val path = if (existingPath.toStr.isEmpty) structFieldInfo.id.toString else s"${existingPath.toStr}/${structFieldInfo.id}"
        val (pfi, optionPrimitiveCol, optParquetThriftEnumInfo, closestPrimitiveChild) = mapData(path)
        val primitiveColumnIndex = optionPrimitiveCol.getOrElse(mapData(pfi.closestPrimitiveChild.get.toVector.mkString("/"))._2.get)

        val fieldNotNullTest = if (pfi.maxRepetitionLevel > 0)
          s"""colReaders($primitiveColumnIndex).asInstanceOf[CacheColumnReader].notNull(${pfi.maxDefinitionLevel},
              recordIdx,
              Array[Int](0),
              Array[Int](${(0 until pfi.maxRepetitionLevel + 2).map(_ => 0).mkString(",")}),
              repetitionOffsets)"""
        else
          s"colReaders($primitiveColumnIndex).asInstanceOf[CacheColumnReader].notNull(${pfi.maxDefinitionLevel}, recordIdx)"

        val classInstance = structUnionFieldInfo.fieldClassTag.runtimeClass.getName.replaceAll("\\$", ".")
        val resultantBuilder = s"""if($fieldNotNullTest){
        val r = $builderStr
        return $classInstance(r)
        }

      """

        (objectCode, resultantBuilder)
    })

    val code = s"""
import com.twitter.lui.column_reader.BaseColumnReader
import com.twitter.lui.column_reader.cache.CacheColumnReader
import com.twitter.lui.column_reader.noncache.NonCacheColumnReader
import com.twitter.lui.scrooge.ScroogeGenerator
import java.util.Arrays
import scala.collection.immutable.List
import scala.collection.immutable.Map
import scala.collection.immutable.Vector
import scala.collection.mutable.Builder
import scala.collection.mutable.MutableList

new Function4[Array[BaseColumnReader], ScroogeGenerator, Long,  Array[Int], $className]{
  def apply(colReaders: Array[BaseColumnReader], scroogeGenerator: ScroogeGenerator , recordIdx: Long, repetitionOffsets: Array[Int]): $className = {
    $fieldSections
    scala.sys.error("Unhandled field")
  }
}
    """
    // println(code)
    // // to stash the generated classes:
    // import java.io._
    // val pw = new PrintWriter(new File(s"/tmp/class_${className}.scala" ))
    // pw.write(code)
    // pw.close
    try {
      compiler.compile[Function4[Array[BaseColumnReader], ScroogeGenerator, Long, Array[Int], T]](code)
    } catch {
      case e: Throwable =>
        println(s"Failed when compiling:\n$code")
        throw e
    }

  }
}