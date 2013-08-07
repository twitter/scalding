/*
Copyright 2013 Twitter, Inc.

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
package com.twitter

package object scalding {
  /**
   * The objects for the Typed-API live in the scalding.typed package
   * but are aliased here.
   */
  val TDsl = com.twitter.scalding.typed.TDsl
  val TypedPipe = com.twitter.scalding.typed.TypedPipe
  type TypedPipe[+T] = com.twitter.scalding.typed.TypedPipe[T]
  type TypedSink[-T] = com.twitter.scalding.typed.TypedSink[T]
  type TypedSource[+T] = com.twitter.scalding.typed.TypedSource[T]
  type KeyedList[+K,+V] = com.twitter.scalding.typed.KeyedList[K,V]
}
