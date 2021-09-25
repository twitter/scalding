package com.twitter.scalding.typed.memory_backend

import java.util.concurrent.atomic.AtomicReference

class AtomicBox[T <: AnyRef](init: T) {
  private[this] val ref = new AtomicReference[T](init)

  def lazySet(t: T): Unit =
    ref.lazySet(t)

  def set(t: T): Unit =
    ref.set(t)

  def swap(t: T): T =
    ref.getAndSet(t)

  /**
   * use a pure function to update the state.
   * fn may be called more than once
   */
  def update[R](fn: T => (T, R)): R = {

    @annotation.tailrec
    def loop(): R = {
      val init = ref.get
      val (next, res) = fn(init)
      if (ref.compareAndSet(init, next)) res
      else loop()
    }

    loop()
  }

  def get(): T = ref.get
}

