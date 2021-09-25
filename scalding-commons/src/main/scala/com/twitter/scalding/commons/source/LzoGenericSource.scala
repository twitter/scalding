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

package com.twitter.scalding.commons.source


import com.twitter.elephantbird.mapreduce.io.BinaryConverter
import com.twitter.scalding._

import cascading.scheme.Scheme

/**
 * Generic source with an underlying GenericScheme that uses the supplied BinaryConverter.
 */
abstract class LzoGenericSource[T] extends FileSource with SingleMappable[T] with TypedSink[T] with LocalTapSource {
  def clazz: Class[T]
  def conv: BinaryConverter[T]
  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])
  override def hdfsScheme = HadoopSchemeInstance(LzoGenericScheme[T](conv, clazz).asInstanceOf[Scheme[_, _, _, _, _]])
}

object LzoGenericSource {
  def apply[T](passedConv: BinaryConverter[T], passedClass: Class[T], paths: String*) =
    new LzoGenericSource[T] {
      override val conv: BinaryConverter[T] = passedConv
      override val clazz = passedClass
      override val hdfsPaths = paths
      override val localPaths = paths
    }
}
