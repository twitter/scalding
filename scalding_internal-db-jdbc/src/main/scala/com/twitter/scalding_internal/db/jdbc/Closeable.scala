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

import java.lang.AutoCloseable
import scala.util.Try

object CloseableHelper {

  implicit class QuietlyCloseable(val c: AutoCloseable) extends AnyVal {
    // attempt to close the resource, ignore if it fails
    def closeQuietly(): Unit = Try(c.close()).getOrElse(Try())
  }
}
