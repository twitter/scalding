/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }

import java.io.{ ByteArrayOutputStream => BOS }
import java.io.{ ByteArrayInputStream => BIS }

import scala.collection.immutable.ListMap
import scala.collection.immutable.HashMap

import com.twitter.algebird.{
  AveragedValue,
  DecayedValue,
  HyperLogLog,
  HyperLogLogMonoid,
  Moments,
  Monoid
}

import com.twitter.chill.config.{ ConfiguredInstantiator, ScalaMapConfig }
import com.twitter.chill.hadoop.HadoopConfig
import com.twitter.chill.hadoop.KryoSerialization

import com.esotericsoftware.kryo.io.{ Input, Output }

import org.apache.hadoop.conf.Configuration

/*
* This is just a test case for Kryo to deal with. It should
* be outside KryoTest, otherwise the enclosing class, KryoTest
* will also need to be serialized
*/
case class TestCaseClassForSerialization(x: String, y: Int)

case class TestValMap(val map: Map[String, Double])
case class TestValHashMap(val map: HashMap[String, Double])

class KryoTest extends WordSpec with Matchers {

  implicit def dateParser: DateParser = DateParser.default

  def getSerialization = {
    val conf = new Configuration
    val chillConf = new HadoopConfig(conf)
    ConfiguredInstantiator.setReflect(chillConf, classOf[serialization.KryoHadoop])
    new KryoSerialization(conf)
  }

  def serObj[T <: AnyRef](in: T) = {
    val khs = getSerialization
    val ks = khs.getSerializer(in.getClass.asInstanceOf[Class[AnyRef]])
    val out = new BOS
    ks.open(out)
    ks.serialize(in)
    ks.close
    out.toByteArray
  }

  def deserObj[T <: AnyRef](cls: Class[_], input: Array[Byte]): T = {
    val khs = getSerialization
    val ks = khs.getDeserializer(cls.asInstanceOf[Class[AnyRef]])
    val in = new BIS(input)
    ks.open(in)
    val fakeInputHadoopNeeds = null
    val res = ks.deserialize(fakeInputHadoopNeeds.asInstanceOf[T])
    ks.close
    res.asInstanceOf[T]
  }
  def singleRT[T <: AnyRef](in: T): T = {
    deserObj[T](in.getClass, serObj(in))
  }

  //These are analogous to how Hadoop will serialize
  def serialize(ins: List[AnyRef]) = {
    ins.map { v => (v.getClass, serObj(v)) }
  }
  def deserialize(input: List[(Class[_], Array[Byte])]) = {
    input.map { tup => deserObj[AnyRef](tup._1, tup._2) }
  }
  def serializationRT(ins: List[AnyRef]) = deserialize(serialize(ins))

  "KryoSerializers and KryoDeserializers" should {
    "round trip for KryoHadoop" in {
      val kryoHadoop = new serialization.KryoHadoop(new HadoopConfig(new Configuration))
      val bootstrapKryo = new serialization.KryoHadoop(new ScalaMapConfig(Map.empty)).newKryo

      val buffer = new Array[Byte](1024 * 1024)
      val output = new Output(buffer)
      bootstrapKryo.writeClassAndObject(output, kryoHadoop)

      val input = new Input(buffer)
      val deserialized = bootstrapKryo.readClassAndObject(input).asInstanceOf[serialization.KryoHadoop]
      deserialized.newKryo
    }

    "round trip any non-array object" in {
      import HyperLogLog._
      implicit val hllmon: HyperLogLogMonoid = new HyperLogLogMonoid(4)
      val test = List(1, 2, "hey", (1, 2), Args("--this is --a --b --test 34"),
        ("hey", "you"),
        ("slightly", 1L, "longer", 42, "tuple"),
        Map(1 -> 2, 4 -> 5),
        0 to 100,
        (0 to 42).toList, Seq(1, 100, 1000),
        Map("good" -> 0.5, "bad" -> -1.0),
        Set(1, 2, 3, 4, 10),
        ListMap("good" -> 0.5, "bad" -> -1.0),
        HashMap("good" -> 0.5, "bad" -> -1.0),
        TestCaseClassForSerialization("case classes are: ", 10),
        TestValMap(Map("you" -> 1.0, "every" -> 2.0, "body" -> 3.0, "a" -> 1.0,
          "b" -> 2.0, "c" -> 3.0, "d" -> 4.0)),
        TestValHashMap(HashMap("you" -> 1.0)),
        Vector(1, 2, 3, 4, 5),
        TestValMap(null),
        Some("junk"),
        DecayedValue(1.0, 2.0),
        Moments(100.0), Monoid.plus(Moments(100), Moments(2)),
        AveragedValue(100, 32.0),
        // Serialize an instance of the HLL monoid
        hllmon.toHLL(42),
        Monoid.sum(List(1, 2, 3, 4).map { hllmon.toHLL(_) }),
        'hai)
        .asInstanceOf[List[AnyRef]]
      serializationRT(test) shouldBe test
      // HyperLogLogMonoid doesn't have a good equals. :(
      singleRT(new HyperLogLogMonoid(5)).bits shouldBe 5
    }
    "handle arrays" in {
      def arrayRT[T](arr: Array[T]): Unit = {
        serializationRT(List(arr)).head
          .asInstanceOf[Array[T]].toList shouldBe (arr.toList)
      }
      arrayRT(Array(0))
      arrayRT(Array(0.1))
      arrayRT(Array("hey"))
      arrayRT(Array((0, 1)))
      arrayRT(Array(None, Nil, None, Nil))
    }
    "handle scala singletons" in {
      val test = List(Nil, None)
      //Serialize each:
      serializationRT(test) shouldBe test
      //Together in a list:
      singleRT(test) shouldBe test
    }
    "handle Date, RichDate and DateRange" in {
      import DateOps._
      implicit val tz: java.util.TimeZone = PACIFIC
      val myDate: RichDate = "1999-12-30T14"
      val simpleDate: java.util.Date = myDate.value
      val myDateRange = DateRange("2012-01-02", "2012-06-09")
      singleRT(myDate) shouldBe myDate
      singleRT(simpleDate) shouldBe simpleDate
      singleRT(myDateRange) shouldBe myDateRange
    }
    "Serialize a giant list" in {
      val bigList = (1 to 100000).toList
      val list2 = deserObj[List[Int]](bigList.getClass, serObj(bigList))
      //Specs, it turns out, also doesn't deal with giant lists well:
      list2.zip(bigList).foreach { case (l, r) => l shouldBe r }
    }
  }
}
