package com.twitter.scalding.parquet.tuple.scheme

import java.util.{ HashMap => JHashMap, Map => JMap }

import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.hadoop.api.{ InitContext, WriteSupport, ReadSupport }
import org.apache.parquet.io.api._
import cascading.flow.FlowProcess
import cascading.scheme.{ Scheme, SinkCall, SourceCall }
import cascading.tap.Tap
import cascading.tuple.Tuple
import com.twitter.bijection.{ Injection, GZippedBase64String }
import com.twitter.chill.KryoInjection
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred._
import org.apache.parquet.hadoop.mapred.{ Container, DeprecatedParquetOutputFormat, DeprecatedParquetInputFormat }
import org.apache.parquet.hadoop.{ ParquetInputFormat, ParquetOutputFormat }
import org.apache.parquet.schema._

import scala.util.{ Failure, Success }

/**
 * Parquet tuple materializer permits to create user defined type record from parquet tuple values
 * @param converter root converter
 * @tparam T User defined value type
 */
class ParquetTupleMaterializer[T](val converter: ParquetTupleConverter[T]) extends RecordMaterializer[T] {
  override def getCurrentRecord: T = converter.currentValue

  override def getRootConverter: GroupConverter = converter
}

/**
 * Parquet read support used by [[org.apache.parquet.hadoop.ParquetInputFormat]] to read values from parquet input.
 * User must define record schema and parquet tuple converter that permits to convert parquet tuple to user defined type
 * For case class types, we provide a macro to generate the schema and read support:
 *  [[com.twitter.scalding.parquet.tuple.macros.Macros.caseClassParquetReadSupport]]
 *
 * @tparam T user defined value type
 */
abstract class ParquetReadSupport[T](val rootSchema: String) extends ReadSupport[T] with Serializable {
  val tupleConverter: ParquetTupleConverter[T]

  lazy val rootType: MessageType = MessageTypeParser.parseMessageType(rootSchema)

  override def init(configuration: Configuration, map: JMap[String, String], messageType: MessageType): ReadContext =
    new ReadContext(rootType)

  override def prepareForRead(configuration: Configuration, map: JMap[String, String], messageType: MessageType,
    readContext: ReadContext): RecordMaterializer[T] =
    new ParquetTupleMaterializer(tupleConverter)
}

class ReadSupportInstanceProxy[T] extends ReadSupport[T] {

  def getDelegateInstance(conf: Configuration): ReadSupport[T] = {
    val readSupport = conf.get(ParquetInputOutputFormat.READ_SUPPORT_INSTANCE)
    require(readSupport != null && !readSupport.isEmpty, "no read support instance is configured")
    val readSupportInstance = ParquetInputOutputFormat.injection.invert(readSupport)

    readSupportInstance match {
      case Success(obj) => obj.asInstanceOf[ReadSupport[T]]
      case Failure(e) => throw e
    }
  }

  override def init(context: InitContext): ReadContext = {
    getDelegateInstance(context.getConfiguration).init(context)
  }

  override def prepareForRead(configuration: Configuration, keyValueMetaData: JMap[String, String], fileSchema: MessageType, readContext: ReadContext): RecordMaterializer[T] = {
    getDelegateInstance(configuration).prepareForRead(configuration, keyValueMetaData, fileSchema, readContext)
  }
}

/**
 * Parquet write support used by [[org.apache.parquet.hadoop.ParquetOutputFormat]] to write values to parquet output.
 * User must provide record schema and a function which permits to write a used defined case class to parquet store with
 * the record consumer and schema definition.
 *
 * For case class value types, we provide a macro to generate the write support, please check
 *  [[com.twitter.scalding.parquet.tuple.macros.Macros.caseClassParquetWriteSupport]]
 *
 * @tparam T user defined value type
 */
abstract class ParquetWriteSupport[T](val rootSchema: String) extends WriteSupport[T] with Serializable {

  var recordConsumer: RecordConsumer = null

  lazy val rootType: MessageType = MessageTypeParser.parseMessageType(rootSchema)

  override def init(configuration: Configuration): WriteContext =
    new WriteSupport.WriteContext(rootType, new JHashMap[String, String])

  override def prepareForWrite(rc: RecordConsumer): Unit = recordConsumer = rc

  override def write(record: T): Unit = writeRecord(record, recordConsumer, rootType)

  def writeRecord(r: T, rc: RecordConsumer, schema: MessageType): Unit
}

object ParquetInputOutputFormat {
  val READ_SUPPORT_INSTANCE = "scalding.parquet.read.support.instance"
  val WRITE_SUPPORT_INSTANCE = "scalding.parquet.write.support.instance"
  val injection: Injection[Any, String] = KryoInjection.andThen(Injection.connect[Array[Byte], GZippedBase64String, String])
}

class ParquetOutputFormatFromWriteSupportInstance[T] extends ParquetOutputFormat[T] {
  override def getWriteSupport(conf: Configuration): WriteSupport[T] = {
    val writeSupport = conf.get(ParquetInputOutputFormat.WRITE_SUPPORT_INSTANCE)
    require(writeSupport != null && !writeSupport.isEmpty, "no write support instance is configured")
    val writeSupportInstance = ParquetInputOutputFormat.injection.invert(writeSupport)
    writeSupportInstance match {
      case Success(obj) => obj.asInstanceOf[WriteSupport[T]]
      case Failure(e) => throw e
    }
  }
}

private class InnerDeprecatedParquetOutputFormat[T] extends DeprecatedParquetOutputFormat[T] {
  this.realOutputFormat = new ParquetOutputFormatFromWriteSupportInstance[T]
}

/**
 * Typed parquet tuple scheme.
 * @param readSupport read support class
 * @param writeSupport write support class
 * @param fp filter predicate
 * @tparam T tuple value type
 */
class TypedParquetTupleScheme[T](val readSupport: ParquetReadSupport[T], val writeSupport: ParquetWriteSupport[T],
  val fp: Option[FilterPredicate] = None)
  extends Scheme[JobConf, RecordReader[AnyRef, Container[T]], OutputCollector[AnyRef, T], Array[AnyRef], Array[AnyRef]] {

  type Output = OutputCollector[AnyRef, T]
  type Reader = RecordReader[AnyRef, Container[T]]
  type TapType = Tap[JobConf, Reader, Output]
  type SourceCallType = SourceCall[Array[AnyRef], Reader]
  type SinkCallType = SinkCall[Array[AnyRef], Output]

  override def sourceConfInit(flowProcess: FlowProcess[_ <: JobConf], tap: TapType, jobConf: JobConf): Unit = {
    fp.map(ParquetInputFormat.setFilterPredicate(jobConf, _))
    jobConf.setInputFormat(classOf[DeprecatedParquetInputFormat[T]])
    jobConf.set(ParquetInputOutputFormat.READ_SUPPORT_INSTANCE, ParquetInputOutputFormat.injection(readSupport))
    ParquetInputFormat.setReadSupportClass(jobConf, classOf[ReadSupportInstanceProxy[_]])
  }

  override def source(flowProcess: FlowProcess[_ <: JobConf], sc: SourceCallType): Boolean = {
    val value: Container[T] = sc.getInput.createValue()

    val hasNext = sc.getInput.next(null, value)

    if (!hasNext) false
    else if (value == null) true
    else {
      val tuple = new Tuple(value.get.asInstanceOf[AnyRef])
      sc.getIncomingEntry.setTuple(tuple)
      true
    }
  }

  override def sinkConfInit(flowProcess: FlowProcess[_ <: JobConf], tap: TapType, jobConf: JobConf): Unit = {
    jobConf.setOutputFormat(classOf[InnerDeprecatedParquetOutputFormat[T]])
    jobConf.set(ParquetInputOutputFormat.WRITE_SUPPORT_INSTANCE, ParquetInputOutputFormat.injection(writeSupport))
  }

  override def sink(flowProcess: FlowProcess[_ <: JobConf], sinkCall: SinkCallType): Unit = {
    val tuple = sinkCall.getOutgoingEntry
    require(tuple.size == 1,
      "TypedParquetTupleScheme expects tuple with an arity of exactly 1, but found " + tuple.getFields)
    val value = tuple.getObject(0).asInstanceOf[T]
    val outputCollector = sinkCall.getOutput
    outputCollector.collect(null, value)
  }
}
