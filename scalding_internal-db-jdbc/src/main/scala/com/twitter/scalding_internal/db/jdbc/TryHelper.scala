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

package com.twitter.scalding_internal.db.jdbc

import scala.util.{ Failure, Success, Try }

object TryHelper {

  implicit class TryWithOnComplete[T](val t: Try[T]) extends AnyVal {
    // attempt a side-effect before returning the original Try if the original Try completes (success or failure)
    // ignore any failures while doing so
    def onComplete(action: => Unit): Try[T] = {
      val effect = (_: Any) => { Try(action); t }
      t.transform(effect, effect)
    }

    // attempt a side-effect before returning the orignal Try if the original Try fails
    // ignore any failures while doing so
    def onFailure(action: => Unit): Try[T] = {
      val effect = (e: Throwable) => { Try(action); Failure(e) }
      t.transform(Try(_), effect)
    }
  }
}
