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

import org.apache.spark.rdd._
import org.apache.spark._

package object source {
  /**
   * implicitly convert a source to an RDD, so you can call RDD functions directly on a source
   * @param src
   * @param sc
   * @param mode
   * @tparam T
   * @return
   */
  implicit def toRDD[T](src: BaseRDD[T])(implicit sc: SparkContext, mode: Mode): RDD[T] = src.toRDD

}
