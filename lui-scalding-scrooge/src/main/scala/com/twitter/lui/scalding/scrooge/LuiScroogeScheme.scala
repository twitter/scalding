package com.twitter.lui.scalding.scrooge

import cascading.flow.FlowProcess
import cascading.scheme.{ Scheme, SinkCall, SourceCall }
import cascading.tap.Tap
import cascading.tuple.Tuple
import com.twitter.lui.hadoop.ReadSupport
import com.twitter.lui.inputformat.MapRedParquetInputFormat
import com.twitter.lui.inputformat.ParquetInputFormat
import com.twitter.lui.scrooge.LuiScroogeReadSupport
import com.twitter.scrooge.ThriftStruct
import org.apache.hadoop.mapred.{ JobConf, RecordReader }
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.hadoop.mapred.Container
import org.apache.parquet.schema.PrimitiveType
import scala.reflect.ClassTag

class LuiScroogeScheme[T >: Null <: ThriftStruct: ClassTag]() extends Scheme[JobConf, RecordReader[Void, Container[T]], Void, Void, Void] {
  override def sourceConfInit(
    flowProcess: FlowProcess[JobConf],
    tap: Tap[JobConf, RecordReader[Void, Container[T]], Void],
    config: JobConf): Unit = {
    config.setInputFormat(classOf[MapRedParquetInputFormat[T]])
    ParquetInputFormat.setReadSupportClass(config, classOf[LuiScroogeReadSupport[T]], implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])
  }

  override def source(
    flowProcess: FlowProcess[JobConf],
    sourceCall: SourceCall[Void, RecordReader[Void, Container[T]]]): Boolean = {
    val container = sourceCall.getInput.createValue()

    val hasNext = sourceCall.getInput.next(null, container)
    if (!hasNext) false
    else if (container == null) true
    else {
      sourceCall.getIncomingEntry.setTuple(new Tuple(container.get))
      true
    }
  }

  override def sink(flowProcess: FlowProcess[JobConf], sinkCall: SinkCall[Void, Void]): Unit = ???
  override def sinkConfInit(flowProcess: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[Void, Container[T]], Void], config: JobConf): Unit = ???
}