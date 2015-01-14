package com.twitter.scalding.commons.thrift

import com.twitter.scalding.typed.OrderedBufferable
import cascading.tuple.hadoop.io.BufferedInputStream
import cascading.tuple.StreamComparator
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.TBase
import java.nio.ByteBuffer
import com.twitter.bijection.Inversion.attempt
import com.twitter.bijection.Bufferable
import org.apache.thrift.transport.TIOStreamTransport

case class ArrayOffset(toInt: Int) extends AnyVal {
  def +(o: LocalOffset) = TotalOffset(toInt + o.toInt)
}
// offset within the current byte buffer
case class LocalOffset(toInt: Int) extends AnyVal
// offset within the array byte
case class TotalOffset(toInt: Int) extends AnyVal
case class Offset(toInt: Int) extends AnyVal
case class Length(toInt: Int) extends AnyVal
case class Remaining(toInt: Int) extends AnyVal
case class ArrayBuf(toArrayBytes: Array[Byte]) extends AnyVal

private[thrift] abstract class TProtocolOrderedBufferable[T] extends OrderedBufferable[T] {
  val minFieldId: Short

  @transient protected lazy val factory = new TBinaryProtocol.Factory //  new TCompactProtocol.Factory

  def hash(t: T) = t.hashCode

  private final def extractAdvanceThriftSize(data: ByteBuffer): (ArrayBuf, TotalOffset, Length) = {
    val (lhsArray, lhsArrayOffset, lhsOffset, lhsRemaining) = (ArrayBuf(data.array), ArrayOffset(data.arrayOffset), LocalOffset(data.position), Remaining(data.remaining))
    val lhsTotalOffset = lhsArrayOffset + lhsOffset
    val lhsLen = Length(data.getInt)

    val newLocalOffset = LocalOffset(lhsOffset.toInt + 4)
    val newGlobalOffset = lhsArrayOffset + newLocalOffset
    data.position(newLocalOffset.toInt)
    (lhsArray, newGlobalOffset, lhsLen)
  }

  def compareBinary(lhs: ByteBuffer, rhs: ByteBuffer): OrderedBufferable.Result = {
    val (lBuf, lOffset, lLength) = extractAdvanceThriftSize(lhs)
    val (rBuf, rOffset, rLength) = extractAdvanceThriftSize(rhs)
    val r = ThriftStreamCompare.compare(lBuf.toArrayBytes, lOffset.toInt, lLength.toInt,
      rBuf.toArrayBytes, rOffset.toInt, rLength.toInt,
      minFieldId, factory)
    if (r < 0) {
      OrderedBufferable.Less
    } else if (r > 0) {
      OrderedBufferable.Greater
    } else {
      OrderedBufferable.Equal
    }
  }
}