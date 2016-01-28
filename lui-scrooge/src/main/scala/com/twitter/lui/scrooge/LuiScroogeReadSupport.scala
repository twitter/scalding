package com.twitter.lui.scrooge

import com.twitter.lui.column_reader._
import com.twitter.lui.hadoop.ReadContext
import com.twitter.lui.hadoop.ReadSupport
import com.twitter.lui.MessageColumnIOFactory
import com.twitter.lui.{ MessageColumnIO, GroupColumnIO, PrimitiveColumnIO, ColumnIO }
import com.twitter.lui.{ RecordReader => ParquetRecordReader }
import com.twitter.algebird.Monoid

import com.twitter.scrooge.ThriftStruct
import com.twitter.scrooge.{ ThriftStructCodec3, ThriftStructMetaData, ThriftStructFieldInfo }

import org.apache.hadoop.conf.Configuration

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.mapred.Container
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.{ MessageType, Type => ParquetSchemaType }
import org.apache.parquet.thrift.struct.ThriftType
import org.apache.parquet.thrift.ThriftMetaData
import org.apache.parquet.VersionParser.ParsedVersion

import scala.collection.JavaConverters._
import scala.collection.mutable.{ Map => MMap }
import scala.reflect.ClassTag

case class MaxLvls(maxDefLvl: Int, maxRepLvl: Int)

object LuiScroogeReadSupport {
  private[this] val cache = MMap[(String, Int), ScroogeGenerator]()

  def getGenerator(className: String, thriftColumns: Map[String, (ParquetFieldInfo, Option[Int], Option[ParquetThriftEnumInfo], Vector[Short])]): ScroogeGenerator =
    cache.synchronized {
      val infoIdentifier = thriftColumns.toList.map(_.hashCode).sorted.hashCode
      cache.getOrElseUpdate((className, infoIdentifier), ScroogeGenerator(thriftColumns))
    }

  def getVersion(createdBy: String): ParsedVersion = {
    import org.apache.parquet.VersionParser
    scala.util.Try(VersionParser.parse(createdBy)).getOrElse(null)
  }

}

class LuiScroogeReadSupport[T >: Null <: ThriftStruct] extends ReadSupport[T] {
  import LuiScroogeReadSupport._

  private[this] def buildMapping(msg: MessageColumnIO): Map[List[String], MaxLvls] = {
    @annotation.tailrec
    def go(toVisit: Seq[ColumnIO], acc: Map[List[String], MaxLvls]): Map[List[String], MaxLvls] = {
      toVisit match {
        case GroupColumnIO(_, _, _, maxRep, maxDef, fPath, _, _, children) :: tail =>
          val newMap = acc + (fPath.toList -> MaxLvls(maxDef, maxRep))
          go(children.toList ::: tail, newMap)
        case PrimitiveColumnIO(_, _, _, _, maxRep, maxDef, fPath, _, _) :: tail =>
          val newMap = acc + (fPath.toList -> MaxLvls(maxDef, maxRep))
          go(tail, newMap)
        case Nil => acc
      }
    }
    go(List(msg.root), Map())
  }

  override def getRecordReader(
    columns: PageReadStore,
    configuration: Configuration,
    createdBy: String,
    fileMetadata: Map[String, String],
    fileSchema: MessageType,
    readContext: ReadContext,
    strictTypeChecking: Boolean): ParquetRecordReader[T] = {

    val messageColumnIO = MessageColumnIOFactory.buildMessageColumnIO(
      readContext,
      fileSchema,
      createdBy,
      strictTypeChecking)

    val mapping = buildMapping(messageColumnIO)

    val thriftMetaData: ThriftMetaData = ThriftMetaData.fromExtraMetaData(fileMetadata.asJava)
    val thriftIdPathToParquetIndex: ParquetMappingToThriftMapping = ThriftIdPathToParquetIndex.get(fileSchema, thriftMetaData.getDescriptor)
    require(thriftIdPathToParquetIndex.m.map(_._1).size == thriftIdPathToParquetIndex.m.map(_._1).distinct.size, "Thrift id's should be distinct!")

    val parquetFileThriftEnumMetadata: Map[ThriftIdPath, ParquetThriftEnumInfo] =
      ThriftEnumMetadata.get(thriftMetaData.getDescriptor).map { case (k, v) => k -> ParquetThriftEnumInfo(v) }

    val ct = classTag

    val className = ct.runtimeClass.getName

    val writerVersion = getVersion(createdBy)

    val thriftIntermediateData: Seq[(ThriftIdPath, ParquetFieldInfo)] =
      thriftIdPathToParquetIndex.m.toSeq.map {
        case (tidPath: ThriftIdPath, pfi: ParquetFieldInfo) =>
          val path = pfi.strPath

          val r = mapping(path.toList)

          val enrichedPfi = pfi.copy(maxDefinitionLevel = r.maxDefLvl, maxRepetitionLevel = r.maxRepLvl)

          if (pfi.parquetSchema.isPrimitive) {

            (tidPath, enrichedPfi)
          } else (tidPath, enrichedPfi)
      }

    // Here we are aiming to capture columns used for zero detection or are inside collections
    // those we just buffer up.
    // Otherwise we can go with a simpler mechanism
    val reUsedLut = thriftIntermediateData.flatMap {
      case (tidPath, pfi) =>
        val multipleHits = pfi.closestPrimitiveChild match {
          case Some(child) if (child != tidPath) => List(tidPath -> 1, child -> 1)
          case _ => List(tidPath -> 1)
        }
        val collectionBased = if (pfi.insideCollection) List(tidPath -> 2) else List()
        multipleHits ++ collectionBased
    }.groupBy(_._1).map {
      case (path, listM) =>
        path -> listM.map(_._2).sum
    }

    val primitiveSet: List[((ThriftIdPath, BaseColumnReader), Int)] = thriftIntermediateData
      .toIterator
      .filter{ case (_, pfi) => pfi.parquetSchema.isPrimitive }
      .map {
        case (tidPath, pfi) =>
          val path = pfi.strPath
          val r = mapping(path.toList)

          val pType = pfi.parquetSchema.asPrimitiveType
          val columnDescriptor = new ColumnDescriptor(
            path.toArray,
            pType.getPrimitiveTypeName,
            pType.getTypeLength,
            r.maxRepLvl,
            r.maxDefLvl)

          val pageReader = columns.getPageReader(columnDescriptor)
          val col = (pfi.thriftType, reUsedLut(tidPath) > 1) match {

            case (_: ThriftType.StringType, true) => cache.StringColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.BoolType, true) => cache.BooleanColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.ByteType, true) => cache.IntColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.I32Type, true) => cache.IntColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.I16Type, true) => cache.IntColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.I64Type, true) => cache.LongColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.DoubleType, true) => cache.DoubleColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.EnumType, true) => cache.StringColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)

            case (_: ThriftType.StringType, false) => noncache.StringColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.BoolType, false) => noncache.BooleanColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.ByteType, false) => noncache.IntColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.I32Type, false) => noncache.IntColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.I16Type, false) => noncache.IntColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.I64Type, false) => noncache.LongColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.DoubleType, false) => noncache.DoubleColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)
            case (_: ThriftType.EnumType, false) => noncache.StringColumnReader(writerVersion, columnDescriptor, pageReader, r.maxDefLvl, r.maxRepLvl)

            case _ => sys.error(s"Missing column reader for : ${pfi.thriftType.getClass} -> ${pfi.thriftType}")
          }
          (tidPath, col)
      }
      .zipWithIndex
      .toList

    val allColData: Array[BaseColumnReader] = primitiveSet.map(_._1._2).toArray
    val primitiveLut: Map[ThriftIdPath, Int] = primitiveSet.map{ case ((path, _), idx) => path -> idx }.toMap

    val thriftColumns: Map[String, (ParquetFieldInfo, Option[Int], Option[ParquetThriftEnumInfo], Vector[Short])] =
      thriftIntermediateData.map {
        case (tidPath, pfi) =>
          tidPath.toVector.mkString("/") -> (pfi, primitiveLut.get(tidPath), parquetFileThriftEnumMetadata.get(tidPath), tidPath.toVector)
      }.toMap

    val scroogeGenerator: ScroogeGenerator = getGenerator(className, thriftColumns)

    val globalMaxRepetitionLevel: Int = allColData.map(_.maxRepetitionLevel).max

    new ParquetRecordReader[T] {
      var recordIdx = 0L
      val emptyArr = new Array[Int](globalMaxRepetitionLevel + 1) // +1 since we normally just index max repetition levels into this
      val builder = scroogeGenerator.buildBuilder[T](className, "", allColData, emptyArr)
      def read: T = {
        val t = builder(recordIdx)
        recordIdx += 1
        t
      }
    }
  }

  override def init(context: InitContext): ReadContext = ReadContext(context.getFileSchema) // no projection yet
}
