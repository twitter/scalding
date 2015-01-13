package com.twitter.scalding.commons.thrift

import com.twitter.scalding.typed.OrderedBinary
import cascading.tuple.hadoop.io.BufferedInputStream
import cascading.tuple.StreamComparator
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.TBase
import java.io.{ ByteArrayInputStream, InputStream, OutputStream }
import org.apache.thrift.transport.TIOStreamTransport

import com.twitter.scalding.serialization.JavaStreamEnrichments._

private[thrift] abstract class TProtocolOrderedBinary[T] extends OrderedBinary[T] {
  val minFieldId: Short

  @transient protected lazy val factory = new TBinaryProtocol.Factory //  new TCompactProtocol.Factory

  def hash(t: T) = t.hashCode

  def compareBinary(lhs: InputStream, rhs: InputStream): OrderedBinary.Result = {
    val leftSize = lhs.readSize
    val rightSize = rhs.readSize
    def makeMarked(size: Int, s: InputStream): InputStream = {
      val ms = if (s.markSupported) s else {
        val buf = new Array[Byte](size)
        s.readFully(buf)
        new ByteArrayInputStream(buf)
      }
      // Make sure we can reset after we read this many bytes
      ms.mark(size)
      ms
    }
    val seekingLeft = makeMarked(leftSize, lhs)
    val seekingRight = makeMarked(rightSize, rhs)

    val res = OrderedBinary.resultFrom(ThriftStreamCompare.compare(seekingLeft, seekingRight,
      minFieldId, factory))

    seekingLeft.reset
    seekingLeft.skipFully(leftSize)
    seekingRight.reset
    seekingRight.skipFully(rightSize)
    res
  }
}
