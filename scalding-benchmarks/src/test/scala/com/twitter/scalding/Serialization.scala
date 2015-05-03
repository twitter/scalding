package com.twitter.scalding.benchmarks

import com.twitter.chill.KryoPool
import com.twitter.scalding.serialization._
import java.io.ByteArrayInputStream
import org.scalacheck.{ Gen => scGen, Arbitrary } // We use scalacheck Gens to generate random scalameter gens.
import org.scalameter.api._
import scala.collection.generic.CanBuildFrom
import scala.language.experimental.macros

trait LowerPriorityImplicit {
  implicit def ordBuf[T]: OrderedSerialization[T] = macro com.twitter.scalding.macros.impl.OrderedSerializationProviderImpl[T]
}

object SerializationBenchmark extends PerformanceTest.Quickbenchmark with LowerPriorityImplicit {
  import JavaStreamEnrichments._

  val sizes = Gen.range("size")(300000, 1500000, 300000)
  val smallSizes = Gen.range("size")(30000, 150000, 30000)

  /**
   * This tends to create ascii strings
   */
  def asciiStringGen: scGen[String] = scGen.parameterized { p =>
    val thisSize = p.rng.nextInt(p.size + 1)
    scGen.const(new String(Array.fill(thisSize)(p.rng.nextInt(128).toByte)))
  }
  def charStringGen: scGen[String] =
    scGen.listOf(scGen.choose(0.toChar, Char.MaxValue)).map(_.mkString)

  // Biases to ascii 80% of the time
  def stringGen: scGen[String] = scGen.frequency((4, asciiStringGen), (1, charStringGen))

  implicit def stringArb: Arbitrary[String] = Arbitrary(stringGen)

  def collection[T, C[_]](size: Gen[Int])(implicit arbT: Arbitrary[T], cbf: CanBuildFrom[Nothing, T, C[T]]): Gen[C[T]] =
    collection[T, C](size, arbT.arbitrary)(cbf)

  def collection[T, C[_]](size: Gen[Int], item: scGen[T])(implicit cbf: CanBuildFrom[Nothing, T, C[T]]): Gen[C[T]] =
    size.map { s =>
      val builder = cbf()
      builder.sizeHint(s)
      // Initialize a fixed random number generator
      val rng = new scala.util.Random("scalding".hashCode)
      val p = scGen.Parameters.default.withRng(rng)

      def get(attempt: Int): T =
        if (attempt > 1000) sys.error("Failed to generate after 100 tries")
        else {
          item(p) match {
            case None => get(attempt + 1)
            case Some(t) => t
          }
        }

      (0 until s).foreach { _ =>
        builder += get(0)
      }
      builder.result()
    }

  def roundTrip[T: Serialization](ts: Iterator[T]): Unit =
    ts.map { t =>
      Serialization.fromBytes(Serialization.toBytes(t)).get
    }.foreach(_ => ())

  def kryoRoundTrip[T](k: KryoPool, ts: Iterator[T]): Unit =
    ts.map { t => k.fromBytes(k.toBytesWithClass(t)) }
      .foreach(_ => ())

  def toArrayOrd[T](t: OrderedSerialization[T]): Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]) = {
      t.compareBinary(new ByteArrayInputStream(a), new ByteArrayInputStream(b)).unsafeToInt
    }
  }
  def toArrayOrd[T](k: KryoPool, ord: Ordering[T]): Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]) =
      ord.compare(k.fromBytes(a).asInstanceOf[T],
        k.fromBytes(b).asInstanceOf[T])
  }

  val longArrayByte: Gen[Array[Byte]] =
    collection[Byte, Array](sizes.map(s => (s / 8) * 8))

  // This is here to make sure the compiler cannot optimize away reads
  var effectInt: Int = 0
  var effectLong: Long = 0L

  performance of "Serialization" in {
    measure method "JavaStreamEnrichments.readInt" in {
      using(longArrayByte) in { a =>
        val length = a.length
        val is = new ByteArrayInputStream(a)
        var ints = length / 4
        while (ints > 0) {
          effectInt ^= is.readInt
          ints -= 1
        }
      }
    }
    measure method "JavaStreamEnrichments.readLong" in {
      using(longArrayByte) in { a =>
        val length = a.length
        val is = new ByteArrayInputStream(a)
        var longs = length / 8
        while (longs > 0) {
          effectLong ^= is.readLong
          longs -= 1
        }
      }
    }
    measure method "UnsignedComparisons.unsignedLongCompare" in {
      using(collection[Long, Array](sizes)) in { a =>
        val max = a.length - 1
        var pos = 0
        while (pos < max) {
          effectInt ^= UnsignedComparisons.unsignedLongCompare(a(pos), a(pos + 1))
          pos += 2
        }
      }
    }
    measure method "normal long compare" in {
      using(collection[Long, Array](sizes)) in { a =>
        val max = a.length - 1
        var pos = 0
        while (pos < max) {
          effectInt ^= java.lang.Long.compare(a(pos), a(pos + 1))
          pos += 2
        }
      }
    }
    measure method "UnsignedComparisons.unsignedInt" in {
      using(collection[Int, Array](sizes)) in { a =>
        val max = a.length - 1
        var pos = 0
        while (pos < max) {
          effectInt ^= UnsignedComparisons.unsignedIntCompare(a(pos), a(pos + 1))
          pos += 2
        }
      }
    }
    measure method "normal int compare" in {
      using(collection[Int, Array](sizes)) in { a =>
        val max = a.length - 1
        var pos = 0
        while (pos < max) {
          effectInt ^= java.lang.Integer.compare(a(pos), a(pos + 1))
          pos += 2
        }
      }
    }
    measure method "UnsignedComparisons.unsignedShort" in {
      using(collection[Short, Array](sizes)) in { a =>
        val max = a.length - 1
        var pos = 0
        while (pos < max) {
          effectInt ^= UnsignedComparisons.unsignedShortCompare(a(pos), a(pos + 1))
          pos += 2
        }
      }
    }
    measure method "normal short compare" in {
      using(collection[Short, Array](sizes)) in { a =>
        val max = a.length - 1
        var pos = 0
        while (pos < max) {
          effectInt ^= java.lang.Short.compare(a(pos), a(pos + 1))
          pos += 2
        }
      }
    }
    measure method "UnsignedComparisons.unsignedByte" in {
      using(collection[Byte, Array](sizes)) in { a =>
        val max = a.length - 1
        var pos = 0
        while (pos < max) {
          effectInt ^= UnsignedComparisons.unsignedByteCompare(a(pos), a(pos + 1))
          pos += 2
        }
      }
    }
    measure method "normal byte compare" in {
      using(collection[Byte, Array](sizes)) in { a =>
        val max = a.length - 1
        var pos = 0
        while (pos < max) {
          effectInt ^= java.lang.Byte.compare(a(pos), a(pos + 1))
          pos += 2
        }
      }
    }
    measure method "typeclass: Int" in {
      using(collection[Int, List](sizes)) in { l => roundTrip(l.iterator) }
    }
    measure method "kryo: Int" in {
      val kryo = KryoPool.withByteArrayOutputStream(1,
        com.twitter.scalding.Config.default.getKryo.get)

      using(collection[Int, List](sizes)) in { l => kryoRoundTrip(kryo, l.iterator) }
    }
    measure method "typeclass: String" in {
      using(collection[String, List](smallSizes)) in { l => roundTrip(l.iterator) }
    }
    measure method "kryo: String" in {
      val kryo = KryoPool.withByteArrayOutputStream(1,
        com.twitter.scalding.Config.default.getKryo.get)

      using(collection[String, List](smallSizes)) in { l => kryoRoundTrip(kryo, l.iterator) }
    }
    measure method "typeclass: (Int, (Long, String))" in {
      using(collection[(Int, (Long, String)), List](smallSizes)) in { l => roundTrip(l.iterator) }
    }
    measure method "kryo: (Int, (Long, String))" in {
      val kryo = KryoPool.withByteArrayOutputStream(1,
        com.twitter.scalding.Config.default.getKryo.get)

      using(collection[(Int, (Long, String)), List](smallSizes)) in { l => kryoRoundTrip(kryo, l.iterator) }
    }
    measure method "typeclass: (Int, Long, Short)" in {
      using(collection[(Int, Long, Short), List](smallSizes)) in { l => roundTrip(l.iterator) }
    }
    measure method "kryo: (Int, Long, Short)" in {
      val kryo = KryoPool.withByteArrayOutputStream(1,
        com.twitter.scalding.Config.default.getKryo.get)

      using(collection[(Int, Long, Short), List](smallSizes)) in { l => kryoRoundTrip(kryo, l.iterator) }
    }
    measure method "sort typeclass: Int" in {
      val ordSer = implicitly[OrderedSerialization[Int]]
      using(collection[Int, List](smallSizes)
        .map { items =>
          items.map { Serialization.toBytes(_) }.toArray
        }) in { ary => java.util.Arrays.sort(ary, toArrayOrd(ordSer)) }
    }
    measure method "sort kryo: Int" in {
      val kryo = KryoPool.withByteArrayOutputStream(1,
        com.twitter.scalding.Config.default.getKryo.get)

      val ord = implicitly[Ordering[Int]]
      using(collection[Int, List](smallSizes)
        .map { items =>
          items.map { kryo.toBytesWithClass(_) }.toArray
        }) in { ary => java.util.Arrays.sort(ary, toArrayOrd(kryo, ord)) }
    }
    measure method "sort typeclass: Long" in {
      val ordSer = implicitly[OrderedSerialization[Long]]
      using(collection[Long, List](smallSizes)
        .map { items =>
          items.map { Serialization.toBytes(_) }.toArray
        }) in { ary => java.util.Arrays.sort(ary, toArrayOrd(ordSer)) }
    }
    measure method "sort kryo: Long" in {
      val kryo = KryoPool.withByteArrayOutputStream(1,
        com.twitter.scalding.Config.default.getKryo.get)

      val ord = implicitly[Ordering[Long]]
      using(collection[Long, List](smallSizes)
        .map { items =>
          items.map { kryo.toBytesWithClass(_) }.toArray
        }) in { ary => java.util.Arrays.sort(ary, toArrayOrd(kryo, ord)) }
    }
    measure method "sort typeclass: String" in {
      val ordSer = implicitly[OrderedSerialization[String]]
      using(collection[String, List](smallSizes)
        .map { items =>
          items.map { Serialization.toBytes(_) }.toArray
        }) in { ary => java.util.Arrays.sort(ary, toArrayOrd(ordSer)) }
    }
    measure method "sort kryo: String" in {
      val kryo = KryoPool.withByteArrayOutputStream(1,
        com.twitter.scalding.Config.default.getKryo.get)

      val ord = implicitly[Ordering[String]]
      using(collection[String, List](smallSizes)
        .map { items =>
          items.map { kryo.toBytesWithClass(_) }.toArray
        }) in { ary => java.util.Arrays.sort(ary, toArrayOrd(kryo, ord)) }
    }

    measure method "sort typeclass: (Int, (Long, String))" in {
      val ordSer = implicitly[OrderedSerialization[(Int, (Long, String))]]
      using(collection[(Int, (Long, String)), List](smallSizes)
        .map { items =>
          items.map { Serialization.toBytes(_) }.toArray
        }) in { ary => java.util.Arrays.sort(ary, toArrayOrd(ordSer)) }
    }
    measure method "sort kryo: (Int, (Long, String))" in {
      val kryo = KryoPool.withByteArrayOutputStream(1,
        com.twitter.scalding.Config.default.getKryo.get)

      val ord = implicitly[Ordering[(Int, (Long, String))]]
      using(collection[(Int, (Long, String)), List](smallSizes)
        .map { items =>
          items.map { kryo.toBytesWithClass(_) }.toArray
        }) in { ary => java.util.Arrays.sort(ary, toArrayOrd(kryo, ord)) }
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
