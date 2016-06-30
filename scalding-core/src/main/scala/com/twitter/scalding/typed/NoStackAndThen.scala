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
  def andThen[C](fn: B => C): NoStackAndThen[A, C] = NoStackAndThen.NoStackMore(this, fn)
  def andThen[C](that: NoStackAndThen[B, C]): NoStackAndThen[A, C] = {
    import NoStackAndThen._
    @annotation.tailrec
    def push(front: NoStackAndThen[A, Any],
      next: NoStackAndThen[Any, Any],
      toAndThen: ReversedStack[Any, C]): NoStackAndThen[A, C] =
      (next, toAndThen) match {
        case (NoStackWrap(fn), EmptyStack(fn2)) => NoStackMore(front, fn).andThen(fn2)
        case (NoStackWrap(fn), NonEmpty(h, tail)) => push(NoStackMore(front, fn), NoStackAndThen.NoStackWrap(h), tail)
        case (NoStackMore(first, tail), _) => push(front, first, NonEmpty(tail, toAndThen))
        case (WithStackTrace(_, _), _) => sys.error("should be unreachable")
      }
    that match {
      case NoStackWrap(fn) => andThen(fn)
      case NoStackMore(head, tail) =>
        // casts needed for the tailrec, they can't cause runtime errors
        push(this, head.asInstanceOf[NoStackAndThen[Any, Any]], EmptyStack(tail))
      case WithStackTrace(inner, stack) => WithStackTrace(andThen(inner), stack)
    }
  }
}

object NoStackAndThen {
  private[typed] def buildStackEntry: Array[StackTraceElement] = Thread.currentThread().getStackTrace

  def apply[A, B](fn: A => B): NoStackAndThen[A, B] = WithStackTrace(NoStackWrap(fn), buildStackEntry)

  private sealed trait ReversedStack[-A, +B]
  private case class EmptyStack[-A, +B](fn: A => B) extends ReversedStack[A, B]
  private case class NonEmpty[-A, B, +C](head: A => B, rest: ReversedStack[B, C]) extends ReversedStack[A, C]

  private[scalding] case class WithStackTrace[A, B](inner: NoStackAndThen[A, B], stackEntry: Array[StackTraceElement]) extends NoStackAndThen[A, B] {
    override def apply(a: A): B = inner(a)

    override def andThen[C](fn: B => C): NoStackAndThen[A, C] =
      WithStackTrace[A, C](inner.andThen(fn), stackEntry ++ buildStackEntry)

    override def andThen[C](that: NoStackAndThen[B, C]): NoStackAndThen[A, C] =
      WithStackTrace[A, C](inner.andThen(that), stackEntry ++ buildStackEntry)
  }

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
    @annotation.tailrec
    private def reversed(toPush: NoStackAndThen[A, Any], rest: ReversedStack[Any, C]): ReversedStack[A, C] =
      toPush match {
        case NoStackWrap(fn) => NonEmpty(fn, rest)
        case NoStackMore(more, fn) => reversed(more, NonEmpty(fn, rest))
        case WithStackTrace(_, _) => sys.error("should be unreachable")
      }
    @annotation.tailrec
    private def call(arg: Any, revstack: ReversedStack[Any, C]): C = revstack match {
      case EmptyStack(last) => last(arg)
      case NonEmpty(head, rest) => call(head(arg), rest)
    }
    private lazy val revStack: ReversedStack[Any, C] =
      // casts needed for the tailrec, they can't cause runtime errors
      reversed(first, EmptyStack(andThenFn.asInstanceOf[(Any) => (C)]))
        .asInstanceOf[ReversedStack[Any, C]]

    def apply(a: A): C = call(a, revStack)
  }
}

