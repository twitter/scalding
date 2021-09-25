package com.twitter.scalding

import com.twitter.bijection.JavaSerializationInjection
import com.twitter.chill.KryoPool
import com.twitter.chill.config.ScalaAnyRefMapConfig
import com.twitter.scalding.serialization.{ Externalizer, KryoHadoop }
import com.twitter.scalding.source.TypedText
import org.scalatest.FunSuite

class ToIterableSerializationTest extends FunSuite {

  class Foo {
    val field = 42
  }

  val myFoo = new Foo
  val testIterableExecution = Execution.toIterable(TypedPipe.from(TypedText.tsv[Int]("foo")).map(_ * myFoo.field))

  test("toIterableExecution should roundtrip") {

    val jInjection = JavaSerializationInjection[Externalizer[Execution[Iterable[Int]]]]
    val externalizer = Externalizer(testIterableExecution)

    assert(jInjection.invert(jInjection(externalizer)).isSuccess)
  }
  test("testing kryo") {
    val kryo = new KryoHadoop(ScalaAnyRefMapConfig(Map("scalding.kryo.setreferences" -> "true")))
    val kryoPool = KryoPool.withByteArrayOutputStream(1, kryo)
    assert(scala.util.Try(kryoPool.deepCopy(testIterableExecution)).isSuccess)
  }

}
