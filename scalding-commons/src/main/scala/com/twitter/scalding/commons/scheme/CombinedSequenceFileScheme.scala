package com.twitter.scalding.commons.scheme

import cascading.scheme.Scheme
import com.twitter.elephantbird.cascading2.scheme.{ CombinedSequenceFile, CombinedWritableSequenceFile }
import com.twitter.scalding.{ HadoopSchemeInstance, SequenceFileScheme, WritableSequenceFileScheme }

trait CombinedSequenceFileScheme extends SequenceFileScheme {
  // TODO Cascading doesn't support local mode yet
  override def hdfsScheme = HadoopSchemeInstance(new CombinedSequenceFile(fields).asInstanceOf[Scheme[_, _, _, _, _]])
}

trait CombinedWritableSequenceFileScheme extends WritableSequenceFileScheme {
  // TODO Cascading doesn't support local mode yet
  override def hdfsScheme =
    HadoopSchemeInstance(new CombinedWritableSequenceFile(fields, keyType, valueType).asInstanceOf[Scheme[_, _, _, _, _]])
}