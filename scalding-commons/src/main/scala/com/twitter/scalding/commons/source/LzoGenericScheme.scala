/*
Copyright 2015 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.scalding.commons.source

import scala.reflect.ClassTag

import com.twitter.bijection._
import com.twitter.chill.Externalizer
import com.twitter.elephantbird.cascading2.scheme.LzoBinaryScheme
import com.twitter.elephantbird.mapreduce.input.combine.DelegateCombineFileInputFormat
import com.twitter.elephantbird.mapreduce.io.{ BinaryConverter, GenericWritable }
import com.twitter.elephantbird.mapreduce.input.{ BinaryConverterProvider, MultiInputFormat }
import com.twitter.elephantbird.mapreduce.output.LzoGenericBlockOutputFormat
import com.twitter.elephantbird.mapred.output.DeprecatedOutputFormatWrapper

import org.apache.hadoop.mapred.{ JobConf, OutputCollector, RecordReader }
import org.apache.hadoop.conf.Configuration

import cascading.tap.Tap
import cascading.flow.FlowProcess

/**
 * Serializes BinaryConverters to JobConf.
 */
private[source] object ExternalizerSerializer {
  def inj[T]: Injection[Externalizer[T], String] = {
    import com.twitter.bijection.Inversion.attemptWhen
    import com.twitter.bijection.codec.Base64

    implicit val baseInj = JavaSerializationInjection[Externalizer[T]]

    implicit val unwrap: Injection[GZippedBase64String, String] =
      // this does not catch cases where it's Base64 but not compressed
      // but the decompression injection will, so it's safe to do this
      new AbstractInjection[GZippedBase64String, String] {
        override def apply(gzbs: GZippedBase64String) = gzbs.str
        override def invert(str: String) = attemptWhen(str)(Base64.isBase64)(GZippedBase64String(_))
      }

    Injection.connect[Externalizer[T], Array[Byte], GZippedBase64String, String]
  }
}

private[source] object ConfigBinaryConverterProvider {
  val ProviderConfKey = "com.twitter.scalding.lzo.converter.provider"
}

/**
 * Provides BinaryConverter serialized in JobConf.
 */
private[source] class ConfigBinaryConverterProvider[M] extends BinaryConverterProvider[M] {
  import ConfigBinaryConverterProvider._

  private[this] var cached: Option[(String, BinaryConverter[M])] = None

  override def getConverter(conf: Configuration): BinaryConverter[M] = {
    val data = conf.get(ProviderConfKey)
    require(data != null, s"$ProviderConfKey is not set in configuration")
    cached match {
      case Some((d, conv)) if d == data => conv
      case _ =>
        val extern = ExternalizerSerializer.inj.invert(data).get
        val conv = extern.get.asInstanceOf[BinaryConverter[M]]
        cached = Some((data, conv))
        conv
    }
  }
}

object LzoGenericScheme {
  def apply[M: ClassTag](conv: BinaryConverter[M]): LzoGenericScheme[M] =
    new LzoGenericScheme(conv, implicitly[ClassTag[M]].runtimeClass.asInstanceOf[Class[M]])

  def apply[M](conv: BinaryConverter[M], clazz: Class[M]): LzoGenericScheme[M] =
    new LzoGenericScheme(conv, clazz)

  /**
   * From a Binary Converter passed in configure in the JobConf using of that by ElephantBird
   */
  def setConverter[M](conv: BinaryConverter[M], conf: JobConf, overrideConf: Boolean = false): Unit = {
    if ((conf.get(ConfigBinaryConverterProvider.ProviderConfKey) == null) || overrideConf) {
      val extern = Externalizer(conv)
      try {
        ExternalizerSerializer.inj.invert(ExternalizerSerializer.inj(extern)).get
      } catch {
        case e: Exception => throw new RuntimeException("Unable to roundtrip the BinaryConverter in the Externalizer.", e)
      }
      conf.set(ConfigBinaryConverterProvider.ProviderConfKey, ExternalizerSerializer.inj(extern))
    }
  }

}

/**
 * Generic scheme for data stored as lzo-compressed protobuf messages.
 * Serialization is performed using the supplied BinaryConverter.
 */
class LzoGenericScheme[M](@transient conv: BinaryConverter[M], clazz: Class[M]) extends LzoBinaryScheme[M, GenericWritable[M]] {

  override protected def prepareBinaryWritable(): GenericWritable[M] =
    new GenericWritable(conv)

  override def sourceConfInit(fp: FlowProcess[JobConf],
    tap: Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]],
    conf: JobConf): Unit = {

    LzoGenericScheme.setConverter(conv, conf)
    MultiInputFormat.setClassConf(clazz, conf)
    MultiInputFormat.setGenericConverterClassConf(classOf[ConfigBinaryConverterProvider[_]], conf)

    DelegateCombineFileInputFormat.setDelegateInputFormat(conf, classOf[MultiInputFormat[_]])
  }

  override def sinkConfInit(fp: FlowProcess[JobConf],
    tap: Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]],
    conf: JobConf): Unit = {
    LzoGenericScheme.setConverter(conv, conf)
    LzoGenericBlockOutputFormat.setClassConf(clazz, conf)
    LzoGenericBlockOutputFormat.setGenericConverterClassConf(classOf[ConfigBinaryConverterProvider[_]], conf)
    DeprecatedOutputFormatWrapper.setOutputFormat(classOf[LzoGenericBlockOutputFormat[_]], conf)
  }
}

