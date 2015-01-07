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

abstract class TBaseOrderedBufferable[T <: TBase[_, _]] extends OrderedBufferable[T] {
  val minFieldId: Short

  @transient private lazy val factory = new TBinaryProtocol.Factory //  new TCompactProtocol.Factory

  @transient protected def prototype: T

  def hash(t: T) = t.hashCode

  /*
    Ideally we would just disable this in memory comparasion, but Task.java in MapReduce deserializes things and uses this to determine if something
    is still the same key. For TBase we could do the full compare here, but not for ThriftStruct(Scrooge) generated code. Since its not comparable.

    TODO: It would be great if the binary comparasion matched in the in memory for both TBase and ThriftStruct.
    In TBase the limitation is that the TProtocol can't tell a Union vs a Struct apart, and those compare differently deserialized
    In ThriftStruct/Scrooge its just not comparable.
    */
  def compare(a: T, b: T) = if (a == b) 0 else -1

  def get(from: java.nio.ByteBuffer): scala.util.Try[(java.nio.ByteBuffer, T)] = attempt(from) { bb =>
    val obj = prototype.deepCopy
    val stream = new com.esotericsoftware.kryo.io.ByteBufferInputStream(bb)
    val len = bb.getInt
    obj.read(factory.getProtocol(new TIOStreamTransport(stream)))
    (bb, obj.asInstanceOf[T])
  }

  def put(into: java.nio.ByteBuffer, t: T): java.nio.ByteBuffer = Bufferable.reallocatingPut(into) { bb =>
    val initialPos = bb.position
    val baos = new com.esotericsoftware.kryo.io.ByteBufferOutputStream(bb)
    bb.putInt(0)
    t.write(factory.getProtocol(new TIOStreamTransport(baos)))
    val endPosition = bb.position

    bb.position(initialPos)
    bb.putInt(endPosition - (initialPos + 4))
    bb.position(endPosition)
    bb
  }

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