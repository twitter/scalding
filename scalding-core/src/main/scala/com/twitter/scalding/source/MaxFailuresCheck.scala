/*
Copyright 2012 Twitter, Inc.

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

package com.twitter.scalding.source

import com.twitter.bijection.Injection
import java.util.concurrent.atomic.AtomicInteger

// TODO: this should actually increment an read a Hadoop counter
class MaxFailuresCheck[T,U](val maxFailures: Int)
  (implicit override val injection: Injection[T,U])
  extends CheckedInversion[T,U] {

  private val failures = new AtomicInteger(0)
  def apply(input: U): Option[T] = {
    try {
      Some(injection.invert(input).get)
    }
    catch {
      case e =>
        // TODO: use proper logging
        e.printStackTrace()
        assert(
          failures.incrementAndGet <= maxFailures,
          "maximum decoding errors exceeded"
        )
        None
    }
  }
}
