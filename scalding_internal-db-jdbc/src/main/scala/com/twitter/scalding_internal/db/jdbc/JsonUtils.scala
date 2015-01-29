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

import scala.util.Try

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization._
import org.json4s.{ NoTypeHints, native }

import com.twitter.bijection.{ Injection, AbstractInjection }
import com.twitter.bijection.Inversion._

object JsonUtils {
  implicit val formats = native.Serialization.formats(NoTypeHints)

  def caseClass2Json[A <: AnyRef](implicit tt: Manifest[A], fmt: Formats): Injection[A, String] = new AbstractInjection[A, String] {
    override def apply(a: A): String = write(a)
    override def invert(b: String): Try[A] = attempt(b)(read[A])
  }
}
