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
  val inj: Injection[Externalizer[_], String] = {
    import com.twitter.bijection.Inversion.attemptWhen
    import com.twitter.bijection.codec.Base64

    implicit val baseInj = JavaSerializationInjection[Externalizer[_]]

    implicit val unwrap: Injection[GZippedBase64String, String] =
      new AbstractInjection[GZippedBase64String, String] {
        override def apply(gzbs: GZippedBase64String) = gzbs.str
        override def invert(str: String) = attemptWhen(str)(Base64.isBase64)(GZippedBase64String(_))
      }

    Injection.connect[Externalizer[_], Array[Byte], GZippedBase64String, String]
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
  override def getConverter(conf: Configuration): BinaryConverter[M] = {
    val data = conf.get(ProviderConfKey)
    require(data != null, s"No data in field $ProviderConfKey")

    val extern: Externalizer[_] = ExternalizerSerializer.inj.invert(data).get
    extern.get.asInstanceOf[BinaryConverter[M]]
  }
}

/**
 * Generic scheme for data stored as lzo-compressed protobuf messages.
 * Serialization is performed using the supplied BinaryConverter.
 */
class LzoGenericScheme[M: ClassTag](@transient conv: BinaryConverter[M]) extends LzoBinaryScheme[M, GenericWritable[M]] {

  override protected def prepareBinaryWritable(): GenericWritable[M] =
    new GenericWritable(conv)

  override def sourceConfInit(fp: FlowProcess[JobConf],
    tap: Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]],
    conf: JobConf): Unit = {

    val extern = Externalizer(conv)
    try {
      ExternalizerSerializer.inj.invert(ExternalizerSerializer.inj(extern)).get
    } catch {
      case e: Exception => throw new RuntimeException("Unable to roundtrip the BinaryConverter in the Externalizer.", e)
    }

    conf.set(ConfigBinaryConverterProvider.ProviderConfKey, ExternalizerSerializer.inj(extern))

    MultiInputFormat.setClassConf(conv.getClass, conf)
    MultiInputFormat.setGenericConverterClassConf(classOf[ConfigBinaryConverterProvider[_]], conf)

    DelegateCombineFileInputFormat.setDelegateInputFormat(conf, classOf[MultiInputFormat[_]])
  }

  override def sinkConfInit(fp: FlowProcess[JobConf],
    tap: Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]],
    conf: JobConf): Unit = {
    val extern = Externalizer(conv)
    try {
      ExternalizerSerializer.inj.invert(ExternalizerSerializer.inj(extern)).get
    } catch {
      case e: Exception => throw new RuntimeException("Unable to roundtrip the BinaryConverter in the Externalizer.", e)
    }

    LzoGenericBlockOutputFormat.setClassConf(implicitly[ClassTag[M]].runtimeClass, conf)
    conf.set(ConfigBinaryConverterProvider.ProviderConfKey, ExternalizerSerializer.inj(extern))
    LzoGenericBlockOutputFormat.setGenericConverterClassConf(classOf[ConfigBinaryConverterProvider[_]], conf)
    DeprecatedOutputFormatWrapper.setOutputFormat(classOf[LzoGenericBlockOutputFormat[_]], conf)
  }
}

