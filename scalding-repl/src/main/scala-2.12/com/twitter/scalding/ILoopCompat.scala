package com.twitter.scalding

import java.io.BufferedReader

import scala.tools.nsc.interpreter.ILoop
import scala.tools.nsc.interpreter.JPrintWriter

class ILoopCompat(in: Option[BufferedReader], out: JPrintWriter)
  extends ILoop(in, out) {
  def addThunk(f: => Unit): Unit = intp.initialize(f)
}
