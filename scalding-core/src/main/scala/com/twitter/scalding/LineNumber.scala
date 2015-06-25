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

object LineNumber {
  /**
   * depth 0 means the StackTraceElement for the caller
   * of this method (skipping getCurrent and the Thread.currentThread
   */
  def getCurrent(depth: Int): StackTraceElement =
    getCurrent(depth, Thread.currentThread().getStackTrace)

  private def getCurrent(depth: Int, stack: Seq[StackTraceElement]): StackTraceElement =
    stack(depth + 2)

  def ignorePath(classPrefix: String): Option[StackTraceElement] =
    ignorePath(classPrefix, Thread.currentThread().getStackTrace)

  private def ignorePath(classPrefix: String, stack: Seq[StackTraceElement]): Option[StackTraceElement] =
    stack.drop(2)
      .dropWhile(_.getClassName.startsWith(classPrefix))
      .headOption

  /*
   * If you use this method, it will try to give you the non-scalding
   * caller of the current method. It does this by ignoring all callers
   * in com.twitter.scalding.* unless the caller is a Job (to make testing
   * easier). Otherwise it just gets the most direct
   * caller for methods that have all the callers in the scalding package
   */
  def tryNonScaldingCaller: StackTraceElement = {
    /* depth = 1:
     * depth 0 => tryNonScaldingCaller
     * depth 1 => caller of this method
     */
    val stack = Thread.currentThread().getStackTrace
    val scaldingPrefix = "com.twitter.scalding."
    val nonScalding = ignorePath(scaldingPrefix, stack)
    val jobClass = classOf[com.twitter.scalding.Job]

    // there is no .headOption on Iterator. WTF?
    def headOption[T](it: Iterator[T]): Option[T] =
      if (it.hasNext) Some(it.next)
      else None

    val scaldingJobCaller = headOption(stack
      .iterator
      .filter { se => se.getClassName.startsWith(scaldingPrefix) }
      .filter { se =>
        val cls = Class.forName(se.getClassName)
        jobClass.isAssignableFrom(cls)
      })

    val directCaller = getCurrent(1, stack)

    scaldingJobCaller
      .orElse(nonScalding)
      .getOrElse(directCaller)
  }
}
