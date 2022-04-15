package com.twitter.scalding.serialization

import com.twitter.scalding.typed.MultiJoinFunction

object MultiJoinExternalizer {
  import MultiJoinFunction.Transformer

  final case class ExternalizeMapGroup[A, B, C](@transient fn: (A, Iterator[B]) => Iterator[C])
      extends Function2[A, Iterator[B], Iterator[C]] {
    private[this] val fnEx = Externalizer(fn)

    def apply(a: A, it: Iterator[B]) = fnEx.get(a, it)
  }

  final case class ExternalizeJoin[A, B, C, D](@transient fn: (A, Iterator[B], Iterable[C]) => Iterator[D])
      extends Function3[A, Iterator[B], Iterable[C], Iterator[D]] {
    private[this] val fnEx = Externalizer(fn)

    def apply(a: A, bs: Iterator[B], cs: Iterable[C]) = fnEx.get(a, bs, cs)
  }

  private[this] object ExtTrans extends Transformer {
    def transformJoin[A, B, C, D](
        fn: (A, Iterator[B], Iterable[C]) => Iterator[D]
    ): (A, Iterator[B], Iterable[C]) => Iterator[D] =
      ExternalizeJoin(fn)
    def transformMap[A, B, C](fn: (A, Iterator[B]) => Iterator[C]): (A, Iterator[B]) => Iterator[C] =
      ExternalizeMapGroup(fn)
  }

  def externalize[A, B](mjf: MultiJoinFunction[A, B]): MultiJoinFunction[A, B] =
    ExtTrans(mjf)
}
