package com.twitter.scalding.parquet.thrift

import cascading.flow.FlowProcess
import cascading.tap.Tap
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import org.apache.parquet.cascading.{ ParquetTBaseScheme, ParquetValueScheme }
import org.apache.parquet.hadoop.thrift.ThriftReadSupport
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.schema.MessageType
import org.apache.parquet.thrift.struct.ThriftType.StructType.StructOrUnionType
import org.apache.parquet.thrift.struct.ThriftType._
import org.apache.parquet.thrift.struct.{ ThriftField, ThriftType }
import org.apache.parquet.thrift.{ ThriftReader, ThriftRecordConverter }
import org.apache.thrift.TBase
import org.apache.thrift.protocol.TProtocol

import scala.collection.JavaConverters._

/**
 * This file contains workarounds for PARQUET-346, everything in it should
 * be removed once that bug is fixed in upstream parquet.
 *
 * The root issue is that TBaseRecordConverter passes a schema
 * based on the file metadata to ThriftRecordConverter that may be missing
 * structOrUnionType metadata. This metadata is not actually needed, but parquet
 * currently throws if it's missing. The (temporary) "fix" is to populate this metadata
 * by setting all structOrUnionType fields to UNION.
 */

/**
 * The same as ParquetTBaseScheme, but sets the record convert to Parquet346TBaseRecordConverter
 */
class Parquet346TBaseScheme[T <: TBase[_, _]](config: ParquetValueScheme.Config[T])
  extends ParquetTBaseScheme[T](config) {

  override def sourceConfInit(fp: FlowProcess[_ <: JobConf],
    tap: Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]],
    jobConf: JobConf): Unit = {

    super.sourceConfInit(fp, tap, jobConf)

    // Use the fixed record converter instead of the one set in super
    ThriftReadSupport.setRecordConverterClass(jobConf, classOf[Parquet346TBaseRecordConverter[_]])
  }
}

/**
 * Same as TBaseRecordConverter with one important (subtle) difference.
 * It passes a repaired schema (StructType) to ThriftRecordConverter's
 * constructor. This is important because older files don't contain all the metadata needed for
 * ThriftSchemaConverter to not throw, but we can put dummy data in there because it's not actually
 * used.
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

  // this is the fix -- we add in the missing structOrUnionType metadata
  // before passing it along
  Parquet346StructTypeRepairer.repair(thriftType))

/**
 * Takes a ThriftType with potentially missing structOrUnionType metadata,
 * and makes a copy that sets all StructOrUnionType metadata to UNION
 */
object Parquet346StructTypeRepairer extends StateVisitor[ThriftType, Unit] {

  def repair(fromMetadata: StructType): StructType = {
    visit(fromMetadata, ())
  }

  def copyRecurse(field: ThriftField): ThriftField = {
    new ThriftField(field.getName, field.getFieldId, field.getRequirement, field.getType.accept(this, ()))
  }

  override def visit(structType: StructType, state: Unit): StructType = {
    val repairedChildren = structType
      .getChildren
      .asScala
      .iterator
      .map(copyRecurse)

    new StructType(repairedChildren.toBuffer.asJava, StructOrUnionType.UNION)
  }

  override def visit(mapType: MapType, state: Unit): MapType =
    new MapType(copyRecurse(mapType.getKey), copyRecurse(mapType.getValue))

  override def visit(setType: SetType, state: Unit): SetType =
    new SetType(copyRecurse(setType.getValues))

  override def visit(listType: ListType, state: Unit): ListType =
    new ListType(copyRecurse(listType.getValues))

  override def visit(enumType: EnumType, state: Unit): EnumType = enumType

  override def visit(boolType: BoolType, state: Unit): BoolType = boolType

  override def visit(byteType: ByteType, state: Unit): ByteType = byteType

  override def visit(doubleType: DoubleType, state: Unit): DoubleType = doubleType

  override def visit(i16Type: I16Type, state: Unit): I16Type = i16Type

  override def visit(i32Type: I32Type, state: Unit): I32Type = i32Type

  override def visit(i64Type: I64Type, state: Unit): I64Type = i64Type

  override def visit(stringType: StringType, state: Unit): StringType = stringType
}
