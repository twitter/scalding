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
package com.twitter.scalding.macros.impl

import scala.language.experimental.macros
import scala.reflect.macros.Context
import com.twitter.scalding.typed.{ MapFnWithDescription, FlatFnWithDescription }
import com.twitter.scalding._
/**
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */
private[macros] object AnnotatedFunctionImpl {
  case class Location(className: String, methodName: String, line: Int, source: String)
  def genLocation(c: Context): Location = {
    import c.universe._
    val className = Option(c.enclosingClass).map(_.symbol.toString).getOrElse("").replaceAll(raw"^\s*class\s*", "")
    val methodName = Option(c.enclosingMethod).map(_.symbol.toString).getOrElse("")
    val line = c.enclosingPosition.line
    val source = c.enclosingPosition.source.toString
    Location(className, methodName, line, source)
  }
  def flatMapFn[T, U](c: Context)(fn: c.Expr[T => TraversableOnce[U]])(implicit T: c.WeakTypeTag[T], U: c.WeakTypeTag[U]): c.Expr[FlatFnWithDescription[T, U]] = {
    import c.universe._
    val location = genLocation(c)
    val tContainer = newTermName(c.fresh(s"t"))
    val description = s"flatMap[${T.tpe} => ${U.tpe}]{${fn.tree}} @ ${location.source}:${location.className}:${location.methodName}:${location.line}"
    val res = q"""
    new _root_.com.twitter.scalding.typed.FlatFnWithDescription[$T, $U] {
      def fn($tContainer: $T) = $fn($tContainer)
      def descr = $description
    }
    """
    c.Expr[FlatFnWithDescription[T, U]](res)
  }

  def mapFn[T, U](c: Context)(fn: c.Expr[T => U])(implicit T: c.WeakTypeTag[T], U: c.WeakTypeTag[U]): c.Expr[MapFnWithDescription[T, U]] = {
    import c.universe._
    val location = genLocation(c)
    val tContainer = newTermName(c.fresh(s"t"))
    val description = s"map[${T.tpe} => ${U.tpe}]{${fn.tree}} @ ${location.source}:${location.className}:${location.methodName}:${location.line}"
    val res = q"""
    new _root_.com.twitter.scalding.typed.MapFnWithDescription[$T, $U] {
      def fn($tContainer: $T) = $fn($tContainer)
      def descr = $description
    }
    """
    c.Expr[MapFnWithDescription[T, U]](res)
  }

}

