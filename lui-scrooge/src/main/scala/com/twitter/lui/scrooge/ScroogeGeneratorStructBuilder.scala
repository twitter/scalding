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

object ScroogeGeneratorStructBuilder {
  import ScroogeGenerator._

  private[scrooge] def apply[T <: ThriftStruct](mapData: Map[String, (ParquetFieldInfo, Option[Int], Option[ParquetThriftEnumInfo], Vector[Short])],
    mainClass: Class[T],
    metadata: List[ThriftStructFieldInfo],
    existingPath: ExistingPath,
    cols: Array[BaseColumnReader])(implicit compiler: CustomCompiler): ((Array[BaseColumnReader], ScroogeGenerator, Long, Array[Int]) => T) = {

    val className = mainClass.getName

    val structFieldInfo = mapData(existingPath.toStr)._1
    val (closestPrimitiveFieldInfo, Some(closestPrimitiveColumnIdx), optiEnumInfo, _) = mapData(structFieldInfo.closestPrimitiveChild.get.toVector.mkString("/"))

    val repetitionLevel = structFieldInfo.maxRepetitionLevel

    val enumHelpers = ScroogeGeneratorEnumHelper(metadata, existingPath, cols, mapData)

    val (objectSections: String, fieldSections: String) = Monoid.sum(metadata.zipWithIndex.map {
      case (f: ThriftStructFieldInfo, idx) =>

        val (objectCode: String, builderStr: String) = f.toGenT match {
          case PrimitiveT => ScroogeGeneratorPrimitiveHelper(f, existingPath, cols, mapData, idx)

          case StructT => ScroogeGeneratorStructHelper(f, existingPath, cols, mapData, idx)

          case ListT => ScroogeGeneratorListSetHelper(f, existingPath, cols, mapData, idx)

          case SetT => ScroogeGeneratorListSetHelper(f, existingPath, cols, mapData, idx)

          case MapT => ScroogeGeneratorMapHelper(f, existingPath, cols, mapData, idx)
        }

        (objectCode, builderStr)
    })

    val repetitionCopy = if (structFieldInfo.maxRepetitionLevel > 0)
      "Arrays.copyOf(repetitionOffsets, repetitionOffsets.length)"
    else
      "repetitionOffsets"

    val subclassName = s"ClassInst${scala.math.abs(className.hashCode)}"

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

object ${subclassName} {
  $enumHelpers
  $objectSections
}
class ${subclassName}(colReaders: Array[BaseColumnReader], scroogeGenerator: ScroogeGenerator, recordIdx: Long , repetitionOffsets: Array[Int]) extends $className {
  import ${subclassName}._
  $fieldSections

  override lazy val hashCode: Int = super.hashCode
}
new Function4[Array[BaseColumnReader], ScroogeGenerator, Long,  Array[Int], $className]{
  def apply(colReaders: Array[BaseColumnReader], scroogeGenerator: ScroogeGenerator , recordIdx: Long, repetitionOffsets: Array[Int]): $className =
    new ${subclassName}(colReaders, scroogeGenerator, recordIdx, $repetitionCopy)
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