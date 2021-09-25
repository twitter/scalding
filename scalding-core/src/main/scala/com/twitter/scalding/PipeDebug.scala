/*
Copyright 2014 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.scalding

import cascading.pipe.{ Pipe, Each }
import cascading.operation.Debug
import cascading.operation.Debug.Output

/**
 * This is a builder for Cascading's Debug object.
 * The default instance is the same default as cascading's new Debug()
 * https://github.com/cwensel/cascading/blob/wip-2.5/cascading-core/src/main/java/cascading/operation/Debug.java#L46
 * This is based on work by: https://github.com/granthenke
 * https://github.com/twitter/scalding/pull/559
 */
case class PipeDebug(output: Output = Output.STDERR,
  prefix: String = null,
  printFieldsEvery: Option[Int] = None,
  printTuplesEvery: Int = 1) {

  def toStdOut: PipeDebug = copy(output = Output.STDOUT)
  def toStdErr: PipeDebug = copy(output = Output.STDERR)
  def withPrefix(p: String): PipeDebug = copy(prefix = p)
  // None means never print
  def printFieldsEvery(i: Option[Int]): PipeDebug = copy(printFieldsEvery = i)
  def printTuplesEvery(i: Int): PipeDebug = copy(printTuplesEvery = i)

  def toDebug: Debug = {
    val debug = new Debug(output, prefix, printFieldsEvery.isDefined)
    printFieldsEvery.foreach { x =>
      debug.setPrintFieldsEvery(x)
    }
    debug.setPrintTupleEvery(printTuplesEvery)
    debug
  }

  def apply(p: Pipe): Pipe = new Each(p, toDebug)
}

