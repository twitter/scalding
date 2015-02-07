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
package com.twitter.scalding_internal.db.macros.impl.upstream.scalding

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding_internal.db.macros.impl.upstream.CaseClassFieldSetter

/**
 * Helper class for setting case class fields in cascading Tuple
 */
private[macros] object TupleFieldSetter extends CaseClassFieldSetter {

  def nothing(c: Context)(idx: Int, tree: c.Tree): c.Tree = {
    import c.universe._
    q"""$tree.set($idx, null)"""
  }

  def default(c: Context)(idx: Int, tree: c.Tree, vTree: c.Tree): c.Tree = {
    import c.universe._
    q"""$tree.set($idx, $vTree)"""
  }

  def from(c: Context)(tp: c.Type, idx: Int, tree: c.Tree, vTree: c.Tree,
    allowUnknownTypes: Boolean): c.Tree = {
    import c.universe._

    def simpleType(accessor: Tree) = q"""${accessor}(${idx}, $vTree)"""

    tp match {
      case tpe if tpe =:= typeOf[String] => simpleType(q"$tree.setString")
      case tpe if tpe =:= typeOf[Boolean] => simpleType(q"$tree.setBoolean")
      case tpe if tpe =:= typeOf[Short] => simpleType(q"$tree.setShort")
      case tpe if tpe =:= typeOf[Int] => simpleType(q"$tree.setInteger")
      case tpe if tpe =:= typeOf[Long] => simpleType(q"$tree.setLong")
      case tpe if tpe =:= typeOf[Float] => simpleType(q"$tree.setFloat")
      case tpe if tpe =:= typeOf[Double] => simpleType(q"$tree.setDouble")
      case tpe if allowUnknownTypes => default(c)(idx, tree, vTree)
      // REVIEW: how does this work with AnyVal case classes?
      case _ => c.abort(c.enclosingPosition, s"Unsupported AnyVal type {tp}")
    }
  }
}
