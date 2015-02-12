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
package com.twitter.scalding_internal.db.macros.impl.upstream.scalding

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._
import com.twitter.scalding_internal.db.macros.upstream.bijection.{ IsCaseClass, MacroGenerated }
import com.twitter.scalding_internal.db.macros.impl.upstream.CaseClassBasedSetterImpl
import com.twitter.scalding_internal.db.macros.impl.upstream.bijection.IsCaseClassImpl

/**
 * Generates cascading Tuple from case class
 */
private[macros] object TupleSetterImpl {

  def caseClassTupleSetterImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] =
    caseClassTupleSetterCommonImpl(c, false)

  def caseClassTupleSetterWithUnknownImpl[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] =
    caseClassTupleSetterCommonImpl(c, true)

  def caseClassTupleSetterCommonImpl[T](c: Context, allowUnknownTypes: Boolean)(implicit T: c.WeakTypeTag[T]): c.Expr[TupleSetter[T]] = {
    import c.universe._

    val tupTerm = newTermName(c.fresh("tup"))
    val (finalIdx, set) = CaseClassBasedSetterImpl(c)(tupTerm, allowUnknownTypes, TupleFieldSetter)
    val res = q"""
    new _root_.com.twitter.scalding.TupleSetter[$T] with _root_.com.twitter.scalding_internal.db.macros.upstream.bijection.MacroGenerated {
      override def apply(t: $T): _root_.cascading.tuple.Tuple = {
        val $tupTerm = _root_.cascading.tuple.Tuple.size($finalIdx)
        $set
        $tupTerm
      }
      override val arity: _root_.scala.Int = $finalIdx
    }
    """
    c.Expr[TupleSetter[T]](res)
  }
}
