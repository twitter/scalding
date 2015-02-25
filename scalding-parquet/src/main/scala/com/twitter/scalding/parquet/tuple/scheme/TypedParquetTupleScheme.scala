package com.twitter.scalding.parquet.tuple.scheme

import java.util.{ HashMap => JHashMap, Map => JMap }

import _root_.parquet.filter2.predicate.FilterPredicate
import _root_.parquet.hadoop.api.ReadSupport.ReadContext
import _root_.parquet.hadoop.api.WriteSupport.WriteContext
import _root_.parquet.hadoop.api.{ ReadSupport, WriteSupport }
import _root_.parquet.hadoop.mapred.{ Container, DeprecatedParquetInputFormat, DeprecatedParquetOutputFormat }
import _root_.parquet.io.api._
import cascading.flow.FlowProcess
import cascading.scheme.{ Scheme, SinkCall, SourceCall }
import cascading.tap.Tap
import cascading.tuple.Tuple
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import parquet.hadoop.{ ParquetInputFormat, ParquetOutputFormat }
import parquet.schema._

/**
 * Parquet tuple materializer permits to create user defined type record from parquet tuple values
 * @param converter root converter
 * @tparam T User defined value type
 */
class ParquetTupleMaterializer[T](val converter: ParquetTupleConverter) extends RecordMaterializer[T] {
  override def getCurrentRecord: T = converter.currentValue.asInstanceOf[T]

  override def getRootConverter: GroupConverter = converter
}

/**
 * Parquet read support used by [[parquet.hadoop.ParquetInputFormat]] to read values from parquet input.
 * User must define record schema and parquet tuple converter that permits to convert parquet tuple to user defined type
 * For case class types, we provide a macro to generate the schema and the tuple converter so that user
 * can define a ParquetReadSupport like this:
 *
 *   case class SampleClass(bool: Boolean, long: Long, float: Float)
 *
 *   class SampleClassReadSupport extends ParquetReadSupport[SampleClass] {
 *     import com.twitter.scalding.parquet.tuple.macros.Macros._
 *     override val tupleConverter: ParquetTupleConverter = caseClassParquetTupleConverter[SampleClass]
 *     override val rootSchema: String = caseClassParquetSchema[SampleClass]
 *   }
 *
 * @tparam T user defined value type
 */
trait ParquetReadSupport[T] extends ReadSupport[T] {
  val tupleConverter: ParquetTupleConverter
  val rootSchema: String

  lazy val rootType: MessageType = MessageTypeParser.parseMessageType(rootSchema)

  override def init(configuration: Configuration, map: JMap[String, String], messageType: MessageType): ReadContext =
    new ReadContext(rootType)

  override def prepareForRead(configuration: Configuration, map: JMap[String, String], messageType: MessageType,
    readContext: ReadContext): RecordMaterializer[T] =
    new ParquetTupleMaterializer(tupleConverter)
}

/**
 * Parquet write support used by [[parquet.hadoop.ParquetOutputFormat]] to write values to parquet output.
 * User must provide record schema and a function which permits to write a used defined case class to parquet store with
 * the record consumer and schema definition.
 *
 *  For case class value types, we provide a macro to generate the write support function so that user
 * can define a ParquetWriteSupport like this:
 *
 *   class SampleClassWriteSupport extends TupleWriteSupport[SampleClassB] {
 *     import com.twitter.scalding.parquet.tuple.macros.Macros._
 *
 *     override def writeRecord(r: SampleClassB, rc: RecordConsumer, schema: MessageType):Unit =
 *        Macros.caseClassWriteSupport[SampleClassB](r, rc, schema)
 *
 *     override val rootSchema: String = caseClassParquetSchema[SampleClassB]
 *   }
 *
 * @tparam T user defined value type
 */
trait ParquetWriteSupport[T] extends WriteSupport[T] {

  var recordConsumer: RecordConsumer = null

  val rootSchema: String

  lazy val rootType: MessageType = MessageTypeParser.parseMessageType(rootSchema)

  override def init(configuration: Configuration): WriteContext =
    new WriteSupport.WriteContext(rootType, new JHashMap[String, String])

  override def prepareForWrite(rc: RecordConsumer): Unit = recordConsumer = rc

  override def write(record: T): Unit = writeRecord(record, recordConsumer, rootType)

  def writeRecord(r: T, rc: RecordConsumer, schema: MessageType): Unit
}

/**
 * Typed parquet tuple scheme.
 * @param readSupport read support class
 * @param writeSupport write support class
 * @param fp filter predicate
 * @tparam T tuple value type
 */
class TypedParquetTupleScheme[T](val readSupport: Class[_], val writeSupport: Class[_],
  val fp: Option[FilterPredicate] = None)
  extends Scheme[JobConf, RecordReader[AnyRef, Container[T]], OutputCollector[AnyRef, T], Array[AnyRef], Array[AnyRef]] {

  type Output = OutputCollector[AnyRef, T]
  type Reader = RecordReader[AnyRef, Container[T]]
  type TapType = Tap[JobConf, Reader, Output]
  type SourceCallType = SourceCall[Array[AnyRef], Reader]
  type SinkCallType = SinkCall[Array[AnyRef], Output]

  override def sourceConfInit(flowProcess: FlowProcess[JobConf], tap: TapType, jobConf: JobConf): Unit = {
    fp.map(ParquetInputFormat.setFilterPredicate(jobConf, _))
    jobConf.setInputFormat(classOf[DeprecatedParquetInputFormat[T]])
    ParquetInputFormat.setReadSupportClass(jobConf, readSupport)
  }

  override def source(flowProcess: FlowProcess[JobConf], sc: SourceCallType): Boolean = {
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

  override def sinkConfInit(flowProcess: FlowProcess[JobConf], tap: TapType, jobConf: JobConf): Unit = {
    jobConf.setOutputFormat(classOf[DeprecatedParquetOutputFormat[T]])
    ParquetOutputFormat.setWriteSupportClass(jobConf, writeSupport)
  }

  override def sink(flowProcess: FlowProcess[JobConf], sinkCall: SinkCallType): Unit = {
    val tuple = sinkCall.getOutgoingEntry
    require(tuple.size == 1,
      "TypedParquetTupleScheme expects tuple with an arity of exactly 1, but found " + tuple.getFields)
    val value = tuple.getObject(0).asInstanceOf[T]
    val outputCollector = sinkCall.getOutput
    outputCollector.collect(null, value)
  }
}
