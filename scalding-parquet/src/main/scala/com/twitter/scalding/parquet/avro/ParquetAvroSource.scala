package com.twitter.scalding.parquet.avro

import _root_.parquet.avro.{AvroWriteSupport, AvroReadSupport}
import _root_.parquet.hadoop.mapred.{DeprecatedParquetOutputFormat, DeprecatedParquetInputFormat, Container}
import _root_.parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import org.apache.avro.Schema
import com.twitter.scalding._
import com.twitter.scalding.avro.AvroSchemaType
import cascading.scheme.{SinkCall, SourceCall, Scheme}
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}
import org.apache.hadoop.io.NullWritable
import cascading.tuple.Fields
import cascading.flow.FlowProcess
import cascading.tap.Tap
import org.apache.avro.specific.SpecificRecord

object ParquetAvroSource {
  def apply[A <: SpecificRecord](source: String)
    (implicit mf: Manifest[A], conv: TupleConverter[A], tset: TupleSetter[A], avroType: AvroSchemaType[A]): ParquetAvroSource[A] = new ParquetAvroSource[A](
    Seq(source),
    schema = Avro.getSchema[A],
    projection = None
  )

  def project[A <: SpecificRecord](source: String, projection: Schema)
                                  (implicit mf: Manifest[A], conv: TupleConverter[A], tset: TupleSetter[A], avroType: AvroSchemaType[A]): ParquetAvroSource[A] = new ParquetAvroSource[A](
    Seq(source),
    schema = Avro.getSchema[A],
    projection = Some(projection)
  )
}

class ParquetAvroSource[A <: SpecificRecord](paths: Seq[String], schema: Schema, projection: Option[Schema])
                               (implicit val mf: Manifest[A], conv: TupleConverter[A], tset: TupleSetter[A], avroType: AvroSchemaType[A])
  extends FixedPathSource(paths: _*) with Mappable[A] with TypedSink[A]  {

  override def hdfsScheme = HadoopSchemeInstance(new ParquetAvroScheme(schema, projection).asInstanceOf[Scheme[_, _, _, _, _]])
  override def converter[U >: A] = TupleConverter.asSuperConverter[A, U](conv)
  override def setter[U <: A] = TupleSetter.asSubSetter[A, U](tset)
}

class ParquetAvroScheme[A](schema: Schema, requestedProjection: Option[Schema] = None)
  extends Scheme[JobConf, RecordReader[NullWritable, Container[A]], OutputCollector[NullWritable, A], Array[AnyRef], Array[AnyRef]] {
  val schemaName = schema.getName
  val schemaString = schema.toString
  val projectionString = requestedProjection.map(_.toString)

  setSinkFields(Fields.FIRST)
  setSourceFields(Fields.FIRST)

  override def retrieveSourceFields(flowProcess: FlowProcess[JobConf], tap: Tap[_, _, _]): Fields = {
    setSourceFields(new Fields(schemaName))
    super.retrieveSourceFields(flowProcess, tap)
  }

  override def sourceConfInit(flowProcess: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[NullWritable, Container[A]], OutputCollector[NullWritable, A]], conf: JobConf): Unit = {
    ParquetInputFormat.setReadSupportClass(conf, classOf[AvroReadSupport[A]])
    conf.setInputFormat(classOf[DeprecatedParquetInputFormat[A]])
    // Constants from AvroReadSupport.java
    conf.set("parquet.avro.read.schema", schemaString)
    projectionString.foreach(conf.set("parquet.avro.projection", _))
  }

  def source(flowProcess: FlowProcess[JobConf], sourceCall: SourceCall[Array[AnyRef], RecordReader[NullWritable, Container[A]]]): Boolean = {
    val value : Container[A] = sourceCall.getInput.createValue()
    if (!sourceCall.getInput().next(null, value)) {
      false
    } else {
      if (value == null) true
      else {
        val t = sourceCall.getIncomingEntry().getTuple
        t.clear()
        t.add(value.get())
        true
      }
    }
  }

  def sinkConfInit(flowProcess: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[NullWritable, Container[A]], OutputCollector[NullWritable, A]], conf: JobConf): Unit = {
    conf.set(ParquetOutputFormat.WRITE_SUPPORT_CLASS, classOf[AvroWriteSupport].getName)
    conf.setOutputFormat(classOf[DeprecatedParquetOutputFormat[A]])
    // Constants from AvroWriteSupport.java
    conf.set("parquet.avro.schema", schemaString)
    conf.set("parquet.compression", "SNAPPY")
    conf.set("parquet.block.size", (256 * 1024 * 1024).toString)
  }

  override def sinkPrepare(flowProcess: FlowProcess[JobConf], sinkCall: SinkCall[Array[AnyRef], OutputCollector[NullWritable, A]]): Unit = {
    sinkCall.setContext(Array[AnyRef](new Schema.Parser().parse(schemaString)))
  }

  def sink(flowProcess: FlowProcess[JobConf], sinkCall: SinkCall[Array[AnyRef], OutputCollector[NullWritable, A]]): Unit = {
    val tupleEntry = sinkCall.getOutgoingEntry()
    val schema = sinkCall.getContext()(0).asInstanceOf[Schema]
    val record : A = tupleEntry.getObject(0).asInstanceOf[A]

    sinkCall.getOutput.collect(null, record)
  }


}
