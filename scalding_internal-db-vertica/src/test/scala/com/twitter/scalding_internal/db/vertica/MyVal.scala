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
package com.twitter.scalding_internal.db.vertica

import com.twitter.scalding_internal.db._
import com.twitter.scalding_internal.db.macros._
import com.twitter.scalding._
import java.util.Date
import com.twitter.scalding.platform._

object MyVal {
  val sample = (0 until 3).map { idx =>
    val optiVal = if (idx % 3 == 0) Some(idx) else None
    MyVal(idx.toLong, idx * 10.23123, idx.toString, idx % 2 == 0, optiVal, new Date(idx * 1000 * 3600L * 24), new Date(idx * 1000 * 3600L * 24))
  }.toList
}

case class MyVal(x: Long, y: Double, @size(20) z: String, a: Boolean, k: Option[Int], bDate: Date, cDateTime: Date)
