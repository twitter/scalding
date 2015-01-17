package com.twitter.scalding.commons.thrift

import com.twitter.scalding.serialization.OrderedSerialization
import cascading.tuple.hadoop.io.BufferedInputStream
import cascading.tuple.StreamComparator
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.TBase
import java.io.{ InputStream, OutputStream }
import org.apache.thrift.transport.TIOStreamTransport

import com.twitter.scalding.serialization.JavaStreamEnrichments._

private[thrift] abstract class TProtocolOrderedSerialization[T] extends OrderedSerialization[T] {
  val minFieldId: Short

  @transient protected lazy val factory = new TBinaryProtocol.Factory //  new TCompactProtocol.Factory

  def hash(t: T) = t.hashCode

  def compareBinary(lhs: InputStream, rhs: InputStream): OrderedSerialization.Result = {
    val leftSize = lhs.readSize
    val rightSize = rhs.readSize

    val seekingLeft = lhs.markOrBuffer(leftSize)
    val seekingRight = rhs.markOrBuffer(rightSize)

    val res = OrderedSerialization.resultFrom(ThriftStreamCompare.compare(seekingLeft, seekingRight,
      minFieldId, factory))

    seekingLeft.reset
    seekingLeft.skipFully(leftSize)
    seekingRight.reset
    seekingRight.skipFully(rightSize)
    res
  }
}
