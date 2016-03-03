package com.twitter.scalding

/**
 * This class is used to assist with testing a TypedPipe
 */
object TypedPipeChecker {

  /*
   * Takes a List and a transform function.
   * The resulting TypedPipe form the transform will be run through asserts
   */
  def checkOutputTransform[T, U, R](input: List[T])(transform: TypedPipe[T] => TypedPipe[U])(assertions: List[U] => R): R =
    assertions(inMemoryToList(transform(TypedPipe.from(input))))

  /*
   * Execute a TypedPipe in memory, convert the resulting Iterator to
   * a list and run it through a function that makes arbitrary
   * assertions on it.
   */
  def checkOutput[T, R](output: TypedPipe[T])(assertions: List[T] => R): R =
    assertions(inMemoryToList(output))

  /**
   * Execute a TypedPipe in memory and return the result as a List
   */
  def inMemoryToList[T](output: TypedPipe[T]): List[T] =
    output
      .toIterableExecution
      .waitFor(Config.unitTestDefault, Local(strictSources = true))
      .get
      .toList

  implicit class InMemoryToListEnrichment[T](val pipe: TypedPipe[T]) extends AnyVal {
    def inMemoryToList: List[T] = TypedPipeChecker.inMemoryToList(pipe)
  }
}
