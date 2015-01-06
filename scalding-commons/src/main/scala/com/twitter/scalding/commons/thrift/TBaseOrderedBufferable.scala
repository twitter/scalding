package com.twitter.scalding.commons.thrift

import com.twitter.scalding.typed.OrderedBufferable
import cascading.tuple.hadoop.io.BufferedInputStream
import cascading.tuple.StreamComparator
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.TBase
import com.esotericsoftware.kryo.io.{ Input => KInput }
import java.nio.ByteBuffer

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
  // TODO: this isn't quite right -- we need min field id for all structs inside this struct
  // so we need to walk the types and check all substructs.

  //ThriftLogEvent._Fields.values()(0).getThriftFieldId;
  val minFieldId: Short

  def hash(t: T) = t.hashCode

  def compare(a: T, b: T) = sys.error("In memory comparasons disabled")

  def get(from: java.nio.ByteBuffer): scala.util.Try[(java.nio.ByteBuffer, T)] = ???
  def put(into: java.nio.ByteBuffer, t: T): java.nio.ByteBuffer = ???

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