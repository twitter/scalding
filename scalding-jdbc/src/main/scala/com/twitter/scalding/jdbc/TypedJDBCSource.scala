package com.twitter.scalding.jdbc

import com.twitter.bijection.Injection
import com.twitter.scalding.Dsl
import com.twitter.scalding.SingleMappable
import cascading.pipe.Pipe
import cascading.tap.Tap
import cascading.tuple.{ Fields, TupleEntry }

abstract class TypedJDBCSource[T] extends JDBCSource with SingleMappable[T] {
  import Dsl._

  // Override this to define how to convert from T to the desired columns.
  val injection: Injection[T, TupleEntry]

  override def transformForRead(pipe: Pipe) =
    pipe.mapTo(fields -> 0) { injection.invert(_: TupleEntry).get }

  override def transformForWrite(pipe: Pipe) =
    pipe.mapTo(0 -> fields) { injection.apply(_: T) }
}