package com.twitter.scalding.typed.memory_backend

import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks
import com.twitter.scalding.{ TypedPipe, Execution, Config, Local, TupleConverter }
import com.twitter.scalding.typed.{ TypedPipeGen, LiteralValue }
import com.twitter.scalding.source.TypedText

class MemoryTest extends FunSuite with PropertyChecks {

  private def mapMatch[K, V](ex: Execution[Iterable[(K, V)]]) = {
    val mm = MemoryMode.empty

    val mkv = ex.waitFor(Config.empty, mm)

    val lkv = ex.waitFor(Config.empty, Local(true))
    assert(mkv.get.toMap == lkv.get.toMap)
  }

  private def sortMatch[A: Ordering](ex: Execution[Iterable[A]]) = {
    val mm = MemoryMode.empty

    val mkv = ex.waitFor(Config.empty, mm)

    val lkv = ex.waitFor(Config.empty, Local(true))
    assert(mkv.get.toList.sorted == lkv.get.toList.sorted)
  }

  test("basic word count") {
    val x = TypedPipe.from(0 until 100)
      .groupBy(_ % 2)
      .sum
      .toIterableExecution

    sortMatch(x)
  }

  test("mapGroup works") {
    val x = TypedPipe.from(0 until 100)
      .groupBy(_ % 2)
      .mapGroup { (k, vs) => Iterator.single(vs.foldLeft(k)(_ + _)) }
      .toIterableExecution

    sortMatch(x)
  }

  test("hashJoin works") {
    val input = TypedPipe.from(0 until 100)
    val left = input.map { k => (k, k % 2) }
    val right = input.map { k => (k, k % 3) }

    sortMatch(left.hashJoin(right).toIterableExecution)
  }

  test("join works") {
    val input = TypedPipe.from(0 until 100)
    val left = input.map { k => (k, k % 2) }
    val right = input.map { k => (k, k % 3) }

    sortMatch(left.join(right).toIterableExecution)
  }

  test("scalding memory mode matches cascading local mode") {
    import TypedPipeGen.genWithIterableSources
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 500)
    //forAll(genWithIterableSources) { pipe => sortMatch(pipe.toIterableExecution) }
  }

  test("some specific past match failures") {
    {
      // this is an attempt to recreate:
      // https://github.com/twitter/scalding/issues/1802
      // but it seems to pass for me. I wonder if:
      // 1) it depends on the functions we plug in, and I am guessing wrong
      // 2) it hits a race condition in either cascading or the memory platform, and thus is hard to
      //    trigger
      // 3) the traps caught some error in cascading (like OOM) but so it silently gave bad data
      // due to a transient error
      //
      import TypedPipe._
      val pipe = WithDescriptionTypedPipe(
        Mapped(
          WithDescriptionTypedPipe(
            CrossPipe(
              WithDescriptionTypedPipe(
                Mapped(
                  WithDescriptionTypedPipe(
                    CrossValue(
                      IterablePipe(
                        List(-2147483648)), LiteralValue(2)),
                    List(("org.scalacheck.Gen$R.map(Gen.scala:237)", true))), { tup: (Int, Int) => tup._2 } /*<function1>*/ ),
                List(("org.scalacheck.Gen$R.map(Gen.scala:237)", true))),
              WithDescriptionTypedPipe(
                TrappedPipe(
                  WithDescriptionTypedPipe(
                    TrappedPipe(
                      WithDescriptionTypedPipe(
                        Mapped(
                          WithDescriptionTypedPipe(
                            CrossPipe(
                              WithDescriptionTypedPipe(
                                MergedTypedPipe(
                                  WithDescriptionTypedPipe(
                                    MergedTypedPipe(
                                      IterablePipe(List(1)),
                                      WithDescriptionTypedPipe(
                                        Fork(
                                          WithDescriptionTypedPipe(
                                            Filter(IterablePipe(List(1)), { x: Int => x > 0 } /*org.scalacheck.GenArities$$Lambda$441/1942591200@68118a69*/ ),
                                            List(("org.scalacheck.Gen$R.map(Gen.scala:237)", true)))),
                                        List(("org.scalacheck.Gen$R.map(Gen.scala:237)", true)))),
                                    List(("org.scalacheck.Gen$R.map(Gen.scala:237)", true))),
                                  WithDescriptionTypedPipe(
                                    Filter(
                                      IterablePipe(List(-1418921823)), { x: Int => x < 0 } /*org.scalacheck.GenArities$$Lambda$441/1942591200@59650892*/ ),
                                    List(("org.scalacheck.Gen$R.map(Gen.scala:237)", true)))),
                                List(("org.scalacheck.Gen$R.map(Gen.scala:237)", true))),
                              IterablePipe(List(-21289660))),
                            List(("org.scalacheck.Gen$R.map(Gen.scala:237)", true))),
                          { tup: (Int, Int) => tup._2 } /*<function1>*/ ),
                        List(("org.scalacheck.Gen$R.map(Gen.scala:237)", true))),
                      TypedText.tsv[Int]("djgiz8e0f6Laqdepo"), implicitly[TupleConverter[Int]]),
                    List(("org.scalacheck.Gen$R.map(Gen.scala:237)", true))),
                  TypedText.tsv[Int]("nd2dwym"), implicitly[TupleConverter[Int]]),
                List(("org.scalacheck.Gen$R.map(Gen.scala:237)", true)))),
            List(("org.scalacheck.Gen$R.map(Gen.scala:237)", true))),
          { tup: (Int, Int) => tup._1 } /*<function1>*/ ),
        List(("org.scalacheck.Gen$R.map(Gen.scala:237)", true)))

      sortMatch(pipe.toIterableExecution)
    }
  }
}
