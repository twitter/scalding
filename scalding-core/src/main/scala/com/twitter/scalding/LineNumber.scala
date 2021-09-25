/*
Copyright 2015 Twitter, Inc.

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

import org.slf4j.{ Logger, LoggerFactory }

object LineNumber {
  /**
   * depth 0 means the StackTraceElement for the caller
   * of this method (skipping getCurrent and the Thread.currentThread
   */
  def getCurrent(depth: Int): StackTraceElement =
    getCurrent(depth, Thread.currentThread().getStackTrace)

  private[this] def getCurrent(depth: Int, stack: Seq[StackTraceElement]): StackTraceElement =
    stack(depth + 2)

  def ignorePath(classPrefix: String): Option[StackTraceElement] = ignorePath(Set(classPrefix))
  def ignorePath(classPrefixes: Set[String]): Option[StackTraceElement] =
    ignorePaths(classPrefixes, Thread.currentThread().getStackTrace)

  private val LOG: Logger = LoggerFactory.getLogger(LineNumber.getClass)

  private[this] def ignorePaths(classPrefixes: Set[String], stack: Seq[StackTraceElement]): Option[StackTraceElement] =
    stack.drop(2)
      .dropWhile { ste =>
        classPrefixes.exists { prefix =>
          ste.getClassName.startsWith(prefix)
        }
      }
      .headOption

  /*
   * If you use this method, it will try to give you the non-scalding
   * caller of the current method. It does this by ignoring all callers
   * in com.twitter.scalding.* unless the caller is a Job (to make testing
   * easier). Otherwise it just gets the most direct
   * caller for methods that have all the callers in the scalding package
   */
  def tryNonScaldingCaller: Option[StackTraceElement] =
    tryNonScaldingCaller(Thread.currentThread().getStackTrace)

  def tryNonScaldingCaller(stack: Array[StackTraceElement]): Option[StackTraceElement] = {
    /* depth = 1:
     * depth 0 => tryNonScaldingCaller
     * depth 1 => caller of this method
     */

    // user code is never in our package, or in scala, but
    // since internal methods often recurse we ignore these
    // in our attempt to get a good line number for the user.
    val scaldingPrefix = "com.twitter.scalding."
    val ignoredPrefixes = Set(scaldingPrefix, "scala.")
    val nonScalding = ignorePaths(ignoredPrefixes, stack)
    val jobClass = classOf[com.twitter.scalding.Job]

    // there is no .headOption on Iterator. WTF?
    def headOption[T](it: Iterator[T]): Option[T] =
      if (it.hasNext) Some(it.next)
      else None

    val scaldingJobCaller = headOption(stack
      .iterator
      .filter { se => se.getClassName.startsWith(scaldingPrefix) }
      .filter { se =>
        try {
          val cls = Class.forName(se.getClassName)
          jobClass.isAssignableFrom(cls)
        } catch {
          // skip classes that we don't find. We seem to run into this for some lambdas on Scala 2.12 in travis
          case cnf: ClassNotFoundException =>
            LOG.warn(s"Skipping $se.getClassName as we can't find the class")
            false
        }
      })

    scaldingJobCaller
      .orElse(nonScalding)
  }
}
