package com.twitter.scalding.benchmarks

import scala.language.experimental.macros
import com.twitter.scalding.serialization._
import com.twitter.chill.KryoPool

import org.scalameter.api._

trait LowerPriorityImplicit {
  implicit def ordBuf[T]: OrderedSerialization[T] =
    macro com.twitter.scalding.macros.impl.OrderedSerializationProviderImpl[T]
}

object SerializationBenchmark extends PerformanceTest.Quickbenchmark with LowerPriorityImplicit {
  val sizes = Gen.range("size")(300000, 1500000, 300000)

  val ranges: Gen[List[Int]] = for {
    size <- sizes
  } yield (0 until size).toList

  def roundTrip[T:Serialization](ts: Iterator[T]): Unit =
    ts.map { t =>
      Serialization.fromBytes(Serialization.toBytes(t)).get
    }.foreach(_ => ())

  def kryoRoundTrip[T](k: KryoPool, ts: Iterator[T]): Unit =
    ts.map { t => k.fromBytes(k.toBytesWithClass(t)) }
      .foreach(_ => ())

  performance of "Serialization" in {
    measure method "typeclass" in {
      using(ranges) in { l => roundTrip(l.iterator) }
    }
    measure method "kryo" in {
      val kryo = KryoPool.withByteArrayOutputStream(1,
        com.twitter.scalding.Config.default.getKryo.get)

      using(ranges) in { l => kryoRoundTrip(kryo, l.iterator) }
    }

    /**
     * TODO:
     * 1) simple case class
     * 2) case class with some nesting and collections
     * 3) sorting of an Array[Array[Byte]] using OrderedSerialization vs Array[T]
     * 4) fastest binary sorting for strings (byte-by-byte, longs, etc...)
     */
  }
}
