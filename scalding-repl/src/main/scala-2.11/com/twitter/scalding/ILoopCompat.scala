package com.twitter.scalding

import scala.tools.nsc.interpreter.ILoop

trait ILoopCompat extends ILoop {
  def addThunk(f: => Unit): Unit = intp.initialize(f)
}
