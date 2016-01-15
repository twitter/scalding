package com.twitter.scalding

import java.io.BufferedReader

import scala.tools.nsc.interpreter.ILoop
import scala.tools.nsc.interpreter.JPrintWriter

trait ILoopCompat(in: Option[BufferedReader], out: JPrintWriter)
  extends ILoop(in, out)
