package com.twitter.lui.scalding.scrooge

import cascading.scheme.Scheme
import com.twitter.scalding._
import com.twitter.scrooge.ThriftStruct
import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import scala.reflect.ClassTag

trait LuiScroogeSource[T >: Null <: ThriftStruct] extends FileSource with SingleMappable[T] with LocalTapSource {
  def ct: ClassTag[T]

  override def hdfsScheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _] = {
    val scheme = new LuiScroogeScheme[T]()(ct)
    HadoopSchemeInstance(scheme.asInstanceOf[Scheme[_, _, _, _, _]])
  }
}

case class FPLuiScroogeSource[T >: Null <: ThriftStruct: ClassTag](paths: String*) extends FixedPathSource(paths: _*) with LuiScroogeSource[T] {
  override def ct = implicitly[ClassTag[T]]
}