package com.twitter.scalding

/**
 * This class is used to assist with testing a TypedPipe
 */
object TypedPipeChecker {
  /*
   * Execute a TypedPipe in memory, convert the resulting Iterator to
   * a list and run it through a function that makes arbitrary
   * assertions on it.
   */
  def checkOutput[T](output: TypedPipe[T])(assertions: List[T] => Unit) =
    assertions(checkOutputInline(output))

  /**
    * Execute a TypedPipe in memory and return the result as a List
    */
  def checkOutputInline[T](output: TypedPipe[T]): List[T] =
    output
      .toIterableExecution
      .waitFor(Config.unitTestDefault, Local(strictSources = true))
      .get
      .toList
}
