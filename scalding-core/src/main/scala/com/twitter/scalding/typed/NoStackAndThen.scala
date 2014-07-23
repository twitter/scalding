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
package com.twitter.scalding.typed

/**
 * This type is used to implement .andThen on a function in a way
 * that will never blow up the stack. This is done to prevent
 * deep scalding TypedPipe pipelines from blowing the stack
 *
 * This may be slow, but is used in scalding at planning time
 */
sealed trait NoStackAndThen[-A, +B] extends java.io.Serializable {
  def apply(a: A): B
  final def andThen[C](fn: B => C): NoStackAndThen[A, C] = NoStackAndThen.NoStackMore(this, fn)
}

object NoStackAndThen {
  def apply[A, B](fn: A => B): NoStackAndThen[A, B] = NoStackWrap(fn)
  private sealed trait ReversedStack[-A, +B]
  private case class EmptyStack[-A, +B](fn: A => B) extends ReversedStack[A, B]
  private case class NonEmpty[-A, B, +C](head: A => B, rest: ReversedStack[B, C]) extends ReversedStack[A, C]

  // Just wraps a function
  private case class NoStackWrap[A, B](fn: A => B) extends NoStackAndThen[A, B] {
    def apply(a: A) = fn(a)
  }
  // This is the defunctionalized andThen
  private case class NoStackMore[A, B, C](first: NoStackAndThen[A, B], andThenFn: (B) => C) extends NoStackAndThen[A, C] {
    /*
     * scala cannot optimize tail calls if the types change.
     * Any call that changes types, we replace that type with Any. These casts
     * can never fail, due to the structure above.
     */
    //private def reversed[T](toPush: NoStackAndThen[A, T], rest: ReversedStack[T, C]): ReversedStack[A, C] =
    @annotation.tailrec
    private def reversed(toPush: NoStackAndThen[A, Any], rest: ReversedStack[Any, C]): ReversedStack[A, C] =
      toPush match {
        case NoStackWrap(fn) => NonEmpty(fn, rest)
        case NoStackMore(more, fn) => reversed(more, NonEmpty(fn, rest))
      }
    //private def call[T](arg: T, revstack: ReversedStack[T, C]): C = revstack match {
    @annotation.tailrec
    private def call(arg: Any, revstack: ReversedStack[Any, C]): C = revstack match {
      case EmptyStack(last) => last(arg)
      case NonEmpty(head, rest) => call(head(arg), rest)
    }
    private lazy val revStack =
      // casts needed for the tailrec, they can't cause runtime errors
      reversed(first, EmptyStack(andThenFn.asInstanceOf[(Any) => (C)]))
        .asInstanceOf[ReversedStack[Any, C]]

    def apply(a: A): C = call(a, revStack)
  }
}

