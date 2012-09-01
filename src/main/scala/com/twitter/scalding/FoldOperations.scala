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
import cascading.tuple.{Tuple => CTuple, TupleEntry}

/** Implements reductions on top of a simple abstraction for the Fields-API
 * We use the f-bounded polymorphism trick to return the type called Self
 * in each operation.
 */
trait FoldOperations[Self <: FoldOperations[Self]] extends ReduceOperations[Self] {
  /*
   *  prefer reduce or mapReduceMap. foldLeft will force all work to be
   *  done on the reducers.  If your function is not associative and
   *  commutative, foldLeft may be required.
   *  BEST PRACTICE: make sure init is an immutable object.
   *  NOTE: init needs to be serializable with Kryo (because we copy it for each
   *    grouping to avoid possible errors using a mutable init object).
   */
  def foldLeft[X,T](fieldDef : (Fields,Fields))(init : X)(fn : (X,T) => X)
                 (implicit setter : TupleSetter[X], conv : TupleConverter[T]) : Self
}
