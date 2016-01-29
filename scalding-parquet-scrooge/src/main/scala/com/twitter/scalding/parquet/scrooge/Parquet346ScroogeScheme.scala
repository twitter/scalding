package com.twitter.scalding.parquet.scrooge

import cascading.flow.FlowProcess
import cascading.tap.Tap
import com.twitter.scalding.parquet.thrift.Parquet346StructTypeRepairer
import com.twitter.scrooge.{ ThriftStruct, ThriftStructCodec }
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import org.apache.parquet.cascading.ParquetValueScheme
import org.apache.parquet.hadoop.thrift.ThriftReadSupport
import org.apache.parquet.schema.MessageType
import org.apache.parquet.thrift.struct.ThriftType.StructType
import org.apache.parquet.thrift.{ ThriftReader, ThriftRecordConverter }
import org.apache.thrift.protocol.TProtocol

import scala.util.control.NonFatal

/**
 * This file contains workarounds for PARQUET-346, everything in it should
 * be removed once that bug is fixed in upstream parquet.
 *
 * The root issue is that ScroogeRecordConverter passes a schema
 * based on the file metadata to ThriftRecordConverter that may be missing
 * structOrUnionType metadata. This metadata is not actually needed, but parquet
 * currently throws if it's missing. The (temporary) "fix" is to populate this metadata
 * by setting all structOrUnionType fields to UNION.
 */

/**
 * The same as ParquetScroogeScheme, but sets the record convert to Parquet346ScroogeRecordConverter
 */
class Parquet346ScroogeScheme[T <: ThriftStruct](config: ParquetValueScheme.Config[T])
  extends ParquetScroogeScheme[T](config) {

  override def sourceConfInit(fp: FlowProcess[_ <: JobConf],
    tap: Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]],
    jobConf: JobConf): Unit = {

    super.sourceConfInit(fp, tap, jobConf)

    // Use the fixed record converter instead of the one set in super
    ThriftReadSupport.setRecordConverterClass(jobConf, classOf[Parquet346ScroogeRecordConverter[_]])
  }
}

object Parquet346ScroogeRecordConverter {

  /**
   * Same as the (private) getCodec in ScroogeRecordConverter
   */
  def getCodec[T <: ThriftStruct](klass: Class[T]): ThriftStructCodec[T] = {

    try {
      val companionClass = Class.forName(klass.getName + "$")
      val companionObject: AnyRef = companionClass.getField("MODULE$").get(null)
      companionObject.asInstanceOf[ThriftStructCodec[T]]
    } catch {
      case NonFatal(e) => throw new RuntimeException("Unable to create ThriftStructCodec", e)
    }

  }
}

/**
 * Same as ScroogeRecordConverter with one important (subtle) difference.
 * It passes a repaired schema (StructType) to ThriftRecordConverter's
 * constructor. This is important because older files don't contain all the metadata needed for
 * ThriftSchemaConverter to not throw, but we can put dummy data in there because it's not actually
 * used.
 */
class Parquet346ScroogeRecordConverter[T <: ThriftStruct](thriftClass: Class[T],
  parquetSchema: MessageType,
  thriftType: StructType) extends ThriftRecordConverter[T](
  // this is a little confusing because it's all being passed to the super constructor

  // this thrift reader is the same as what's in ScroogeRecordConverter's constructor
  new ThriftReader[T] {
    val codec: ThriftStructCodec[T] = Parquet346ScroogeRecordConverter.getCodec(thriftClass)
    def readOneRecord(protocol: TProtocol): T = codec.decode(protocol)
  },

  thriftClass.getSimpleName,
  parquetSchema,

  // this is the fix -- we add in the missing structOrUnionType metadata
  // before passing it along
  Parquet346StructTypeRepairer.repair(thriftType))