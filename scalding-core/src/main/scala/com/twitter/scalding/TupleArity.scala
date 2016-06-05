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

import cascading.tuple.Fields

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

  /**
   * assert that the arity of this setter matches the fields given.
   * if arity == -1, we can't check, and if Fields is not a definite
   * size, (such as Fields.ALL), we also cannot check, so this should
   * only be considered a weak check.
   */
  def assertArityMatches(f: Fields): Unit = {
    //Fields.size == 0 for the indefinite Fields: ALL, GROUP, VALUES, UNKNOWN, etc..
    if (f.size > 0 && arity >= 0) {
      assert(arity == f.size, "Arity of (" + super.getClass + ") is "
        + arity + ", which doesn't match: + (" + f.toString + ")")
    }
  }
}
