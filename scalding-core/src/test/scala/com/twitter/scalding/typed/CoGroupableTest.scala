package com.twitter.scalding.typed

import org.scalatest.FunSuite

class CoGroupableTest extends FunSuite {
  test("CoGroupable.atMostOneValue is consistent") {
    val init = TypedPipe.from(List((1, 2)))

    assert(CoGroupable.atMostOneValue(init.sumByKey))
    assert(CoGroupable.atMostOneValue(init.group.sum))
    assert(CoGroupable.atMostOneValue(init.group.mapValues(_ + 100).sum))
    assert(CoGroupable.atMostOneValue(init.group.forceToReducers.mapValues(_ + 100).sum))
    assert(CoGroupable.atMostOneValue(init.group.forceToReducers.mapValues(_ + 100).sum.mapValues(_ - 100)))
    assert(CoGroupable.atMostOneValue(init.group.forceToReducers.mapValues(_ + 100).sum.filter { case (k, v) => k > v }))
    assert(CoGroupable.atMostOneValue(init.group.mapValues(_ * 2).sum.join(init.group.sum)))

    assert(!CoGroupable.atMostOneValue(init.group))
    assert(!CoGroupable.atMostOneValue(init.group.scanLeft(0)(_ + _)))
    assert(!CoGroupable.atMostOneValue(init.join(init.group.mapValues(_ * 2))))
    assert(!CoGroupable.atMostOneValue(init.group.sum.flatMapValues(List(_))))

    val sum1 = init.sumByKey

    assert(CoGroupable.atMostOneValue(sum1.join(sum1.join(sum1))))
    assert(CoGroupable.atMostOneValue(sum1.join(sum1).join(sum1)))

    assert(!CoGroupable.atMostOneValue(init.join(sum1.join(sum1))))
    assert(!CoGroupable.atMostOneValue(init.join(sum1).join(sum1)))
    assert(!CoGroupable.atMostOneValue(sum1.join(init.join(sum1))))
    assert(!CoGroupable.atMostOneValue(sum1.join(init).join(sum1)))
    assert(!CoGroupable.atMostOneValue(sum1.join(sum1.join(init))))
    assert(!CoGroupable.atMostOneValue(sum1.join(sum1).join(init)))
  }
}
