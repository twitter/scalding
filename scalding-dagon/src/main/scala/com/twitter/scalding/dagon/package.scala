package com.twitter.scalding

/** Collection of graph algorithms */
package object dagon {
  type BoolT[T] = Boolean
  type NeighborFn[T] = T => Iterable[T]
}
