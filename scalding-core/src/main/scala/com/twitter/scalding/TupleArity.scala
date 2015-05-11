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

/**
 * Mixed in to both TupleConverter and TupleSetter to improve arity safety
 * of cascading jobs before we run anything on Hadoop.
 */
trait TupleArity {
  /**
   * Return the arity of product types, should probably only be used implicitly
   * The use case here is to see how many fake field names we need in Cascading
   * to hold an intermediate value for mapReduceMap
   */
  def arity: Int

}
