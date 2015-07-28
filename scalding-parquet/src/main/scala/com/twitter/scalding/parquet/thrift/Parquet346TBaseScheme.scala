package com.twitter.scalding.parquet.thrift

import cascading.flow.FlowProcess
import cascading.tap.Tap
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import org.apache.parquet.cascading.{ ParquetTBaseScheme, ParquetValueScheme }
import org.apache.parquet.hadoop.thrift.ThriftReadSupport
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.schema.MessageType
import org.apache.parquet.thrift.struct.ThriftType
import org.apache.parquet.thrift.{ ThriftReader, ThriftRecordConverter, ThriftSchemaConverter }
import org.apache.thrift.TBase
import org.apache.thrift.protocol.TProtocol

/**
 * This file contains workarounds for PARQUET-346, everything in it should
 * be removed once that bug is fixed in upstream parquet.
 *
 * The root issue is that TBaseRecordConverter passes a schema
 * based on the file metadata to ThriftRecordConverter, but it should
 * pass a schema based on the thrift class used to *read* the file.
 */

/**
 * The same as ParquetTBaseScheme, but sets the record convert to Parquet346TBaseRecordConverter
 */
class Parquet346TBaseScheme[T <: TBase[_, _]](config: ParquetValueScheme.Config[T])
  extends ParquetTBaseScheme[T](config) {

  override def sourceConfInit(fp: FlowProcess[JobConf],
    tap: Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]],
    jobConf: JobConf): Unit = {

    super.sourceConfInit(fp, tap, jobConf)

    // Use the fixed record converter instead of the one set in super
    ThriftReadSupport.setRecordConverterClass(jobConf, classOf[Parquet346TBaseRecordConverter[_]])
  }
}

/**
 * Same as TBaseRecordConverter with one important (subtle) difference.
 * It passes a schema (StructType) based on the thrift class to ThriftRecordConverter's
 * constructor instead of a schema based on what's in the parquet file's metadata.
 * This is important because older files don't contain all the metadata needed for
 * ThriftSchemaConverter to not throw, but we can get that information by converting the thrift
 * class to a schema instead.
 */
class Parquet346TBaseRecordConverter[T <: TBase[_, _]](thriftClass: Class[T],
  requestedParquetSchema: MessageType, thriftType: ThriftType.StructType) extends ThriftRecordConverter[T](
  // this is a little confusing because it's all being passed to the super constructor

  // this thrift reader is the same as what's in ScroogeRecordConverter's constructor
  new ThriftReader[T] {
    override def readOneRecord(protocol: TProtocol): T = {
      try {
        val thriftObject: T = thriftClass.newInstance
        thriftObject.read(protocol)
        thriftObject
      } catch {
        case e: InstantiationException =>
          throw new ParquetDecodingException("Could not instantiate Thrift " + thriftClass, e)
        case e: IllegalAccessException =>
          throw new ParquetDecodingException("Thrift class or constructor not public " + thriftClass, e)
      }
    }
  },
  thriftClass.getSimpleName,
  requestedParquetSchema,

  // this is the fix -- we convert thriftClass to a StructType
  // instead of using the thriftType argument passed to us
  // as it comes from the parquet file and may be missing information
  ThriftSchemaConverter.toStructType(thriftClass))