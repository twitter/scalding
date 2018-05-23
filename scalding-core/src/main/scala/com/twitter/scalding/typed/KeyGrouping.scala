package com.twitter.scalding.typed

import com.twitter.scalding.serialization.{
  OrderedSerialization,
  OrderedSerialization2
}
import scala.math.Ordering

case class KeyGrouping[T](ord: Ordering[T])

object KeyGrouping {
  import scala.language.experimental.macros

  implicit def tuple2[T1, T2](implicit ord1: KeyGrouping[T1],
    ord2: KeyGrouping[T2]): KeyGrouping[(T1, T2)] =
    KeyGrouping[(T1, T2)](
      OrderedSerialization2.maybeOrderedSerialization2(ord1.ord, ord2.ord))

  implicit def ordSerConvert[T](
    ordSer: OrderedSerialization[T]): KeyGrouping[T] = KeyGrouping(ordSer)
  // add unsafe convert as well to make everything compatible? add it somehow in a way with macro log?
  //  implicit def unsafeConvert[T](ord: Ordering[T]): KeyGrouping[T] = KeyGrouping(ord)

  implicit def convert[T]: KeyGrouping[T] = macro KeyGroupingMacro[T]
}
