package com.twitter.scalding

import org.apache.hadoop.hbase.util.Bytes
import cascading.tuple.Tuple


abstract class BytesConverter[T] {
  def apply(bytes: Array[Byte]): T
}

trait HBaseGetter[T] extends TupleGetter[T] { // extends HBaseTypeWrapper[T] {
  
  private val BytesTupleGetter = new TupleGetter[Array[Byte]] {
    def get(tup: Tuple, i: Int): Array[Byte] = tup.getObject(i).asInstanceOf[Array[Byte]]
  }

  
  def adapt(tup: Tuple, i: Int, bytesConverter: BytesConverter[T]): T = {
    println("ADAPTED !!!")
    println("Getter got: ", BytesTupleGetter.get(tup, i))
    println("Converter set: ", bytesConverter(BytesTupleGetter.get(tup, i)))
    bytesConverter(BytesTupleGetter.get(tup, i))
  }
}

trait HBaseConversions extends TupleConversions {

  implicit object HBaseShortGetter extends HBaseGetter[Short] {
    
    object BytesToShort extends BytesConverter[Short] {
      def apply(bytes: Array[Byte]) = Bytes.toShort(bytes)
    }
    def get(tup: Tuple, i: Int): Short = {
      adapt(tup, i, BytesToShort)
    }
  }

  implicit object HBaseFloatGetter extends HBaseGetter[Float] {
    object BytesToFloat extends BytesConverter[Float] {
      def apply(bytes: Array[Byte]) = Bytes.toFloat(bytes)
    }
    def get(tup: Tuple, i: Int): Float = {
      adapt(tup, i, BytesToFloat)
    }
  }

  implicit object HBaseDoubleGetter extends HBaseGetter[Double] {
    object BytesToDouble extends BytesConverter[Double] {
      def apply(bytes: Array[Byte]) = Bytes.toDouble(bytes)
    }
    def get(tup: Tuple, i: Int): Double = {
      adapt(tup, i, BytesToDouble)
    }
  }

  implicit object HBaseIntGetter extends HBaseGetter[Int] {
    object BytesToInt extends BytesConverter[Int] {
      def apply(bytes: Array[Byte]) = Bytes.toInt(bytes)
    }
    def get(tup: Tuple, i: Int): Int = {
      adapt(tup, i, BytesToInt)
    }
  }

  implicit object HBaseLongGetter extends HBaseGetter[Long] {
    object BytesToLong extends BytesConverter[Long] {
      def apply(bytes: Array[Byte]) = Bytes.toLong(bytes)
    }
    def get(tup: Tuple, i: Int): Long = {
      adapt(tup, i, BytesToLong)
    }
  }

  implicit object HBaseStringGetter extends HBaseGetter[String] {
    object BytesToString extends BytesConverter[String] {
      def apply(bytes: Array[Byte]) = Bytes.toString(bytes)
    }
    def get(tup: Tuple, i: Int): String = {
      adapt(tup, i, BytesToString)
    }
  }
}

