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
package com.twitter.scalding

import cascading.pipe.Pipe

/**
 * This object has all the implicit functions and values that are used
 * to make the scalding DSL, which includes the functions for automatically
 * creating cascading.tuple.Fields objects from scala tuples of Strings, Symbols
 * or Ints, as well as the cascading.pipe.Pipe enrichment to RichPipe which
 * adds the scala.collections-like API to Pipe.
 *
 * It's useful to import Dsl._ when you are writing scalding code outside
 * of a Job.
 */
object Dsl extends FieldConversions with java.io.Serializable {
  implicit def pipeToRichPipe(pipe : Pipe) : RichPipe = new RichPipe(pipe)
}
