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
package com.twitter.scalding.db.macros.impl

import scala.reflect.macros.Context

import com.twitter.scalding.macros.impl.CaseClassBasedSetterImpl
import com.twitter.scalding.db.JdbcStatementSetter

/**
 * Generates JDBC PreparedStatement data from case class
 */
private[macros] object JdbcStatementSetterImpl {

  def caseClassJdbcSetterCommonImpl[T](c: Context,
    allowUnknownTypes: Boolean)(implicit T: c.WeakTypeTag[T]): c.Expr[JdbcStatementSetter[T]] = {
    import c.universe._

    val stmtTerm = newTermName(c.fresh("stmt"))
    val (_, setterTerm) = CaseClassBasedSetterImpl(c)(stmtTerm, allowUnknownTypes, JdbcFieldSetter)
    val res = q"""
    new _root_.com.twitter.scalding.db.JdbcStatementSetter[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
      override def apply(t: $T, $stmtTerm: _root_.java.sql.PreparedStatement) = _root_.scala.util.Try {
        $setterTerm
        $stmtTerm
      }
    }
    """
    c.Expr[JdbcStatementSetter[T]](res)
  }
}

