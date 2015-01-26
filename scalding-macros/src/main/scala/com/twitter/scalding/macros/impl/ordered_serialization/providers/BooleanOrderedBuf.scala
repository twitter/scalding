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
package com.twitter.scalding.macros.impl.ordered_serialization.providers

import scala.language.experimental.macros
import scala.reflect.macros.Context

import com.twitter.scalding._

import com.twitter.scalding.macros.impl.ordered_serialization.{ CompileTimeLengthTypes, ProductLike, TreeOrderedBuf }
import CompileTimeLengthTypes._

import java.nio.ByteBuffer
import com.twitter.scalding.serialization.OrderedSerialization

object BooleanOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe =:= c.universe.typeOf[Boolean] => BooleanOrderedBuf(c)(tpe)
  }

  def apply(c: Context)(outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType

      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) =
        q"_root_.java.lang.Byte.compare($inputStreamA.readByte, $inputStreamB.readByte)"

      override def hash(element: ctx.TermName): ctx.Tree =
        q"_root_.com.twitter.scalding.serialization.Hasher.boolean.hash($element)"

      override def put(inputStream: ctx.TermName, element: ctx.TermName) =
        q"$inputStream.writeByte(if($element) (1: Byte) else (0: Byte))"

      override def get(inputStreamA: ctx.TermName): ctx.Tree =
        q"($inputStreamA.readByte == (1: Byte))"

      def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree =
        q"_root_.java.lang.Boolean.compare($elementA, $elementB)"

      override def length(element: Tree): CompileTimeLengthTypes[c.type] =
        ConstantLengthCalculation(c)(1)

      override val lazyOuterVariables: Map[String, ctx.Tree] =
        Map.empty
    }
  }
}

