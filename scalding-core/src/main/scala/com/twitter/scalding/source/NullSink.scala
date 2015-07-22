package com.twitter.scalding.source

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import com.twitter.scalding.typed.TypedSink
import com.twitter.scalding.{ BaseNullSource, Mode, TupleSetter }

/**
 * This can be used to cause cascading to run a flow, but discard
 * the output. The only place this is likely of use is to do some (non-recommended,
 * but sometimes the most expediant way to accomplish some task).
 */
object NullSink extends BaseNullSource with TypedSink[Any] {
  def setter[U <: Any] = TupleSetter.asSubSetter[Any, U](TupleSetter.singleSetter)
}
