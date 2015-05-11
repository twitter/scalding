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

import org.apache.spark.rdd.RDD

class InvalidSourceException(message: String) extends RuntimeException(message)

/**
 * Every source must have a correct toString method.  If you use
 * case classes for instances of sources, you will get this for free.
 * This is one of the several reasons we recommend using cases classes
 *
 * java.io.Serializable is needed if the Source is going to have any
 * methods attached that run on mappers or reducers, which will happen
 * if you implement transformForRead or transformForWrite.
 */
trait Source[T, M <: RDD[T]] extends java.io.Serializable {
}
