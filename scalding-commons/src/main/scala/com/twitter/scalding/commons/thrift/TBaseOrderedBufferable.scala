package com.twitter.scalding.commons.thrift

import com.twitter.scalding.typed.OrderedBufferable
import cascading.tuple.hadoop.io.BufferedInputStream
import cascading.tuple.StreamComparator
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.TBase
import com.esotericsoftware.kryo.io.{ Input => KInput }
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

abstract class TBaseOrderedBufferable[T <: TBase[_, _]] extends OrderedBufferable[T] {
  val minFieldId: Short

  @transient protected def klass: Class[T]

  @transient private lazy val factory = new TCompactProtocol.Factory

  @transient protected lazy val prototype = klass.newInstance

  def hash(t: T) = t.hashCode

  def compare(a: T, b: T) = sys.error("In memory comparasons disabled")

  def get(from: java.nio.ByteBuffer): scala.util.Try[(java.nio.ByteBuffer, T)] = attempt(from) { bb =>
    val obj = prototype.deepCopy
    val stream = new com.esotericsoftware.kryo.io.ByteBufferInputStream(bb)
    obj.read(factory.getProtocol(new TIOStreamTransport(stream)))
    (bb, obj.asInstanceOf[T])
  }

  def put(into: java.nio.ByteBuffer, t: T): java.nio.ByteBuffer = Bufferable.reallocatingPut(into) { bb =>
    val baos = new com.esotericsoftware.kryo.io.ByteBufferOutputStream(bb)
    t.write(factory.getProtocol(new TIOStreamTransport(baos)))
    bb
  }

  private final def extractAdvanceThriftSize(data: ByteBuffer): (ArrayBuf, TotalOffset, Length) = {
    val (lhsArray, lhsArrayOffset, lhsOffset, lhsRemaining) = (ArrayBuf(data.array), ArrayOffset(data.arrayOffset), LocalOffset(data.position), Remaining(data.remaining))
    val lhsTotalOffset = lhsArrayOffset + lhsOffset
    val leftKInputStream = new KInput(lhsArray.toArrayBytes, lhsTotalOffset.toInt, lhsRemaining.toInt)
    val lhsLen = Length(leftKInputStream.readInt(true))
    val kryoRead = leftKInputStream.position - lhsTotalOffset.toInt

    val newLocalOffset = LocalOffset(lhsOffset.toInt + lhsLen.toInt + kryoRead)
    val newGlobalOffset = lhsArrayOffset + newLocalOffset
    data.position(newLocalOffset.toInt)
    (lhsArray, newGlobalOffset, lhsLen)
  }

  def compareBinary(lhs: ByteBuffer, rhs: ByteBuffer): OrderedBufferable.Result = {

    val (lBuf, lOffset, lLength) = extractAdvanceThriftSize(lhs)
    val (rBuf, rOffset, rLength) = extractAdvanceThriftSize(rhs)

    val r = ThriftStreamCompare.compare(lBuf.toArrayBytes, lOffset.toInt, lLength.toInt,
      rBuf.toArrayBytes, rOffset.toInt, rLength.toInt,
      minFieldId, new TCompactProtocol.Factory)

    if (r < 0) {
      OrderedBufferable.Less
    } else if (r > 0) {
      OrderedBufferable.Greater
    } else {
      OrderedBufferable.Equal
    }
  }
}