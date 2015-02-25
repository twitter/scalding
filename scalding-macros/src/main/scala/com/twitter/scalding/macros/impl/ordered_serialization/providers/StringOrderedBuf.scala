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

object StringOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe =:= c.universe.typeOf[String] => StringOrderedBuf(c)(tpe)
  }

  def apply(c: Context)(outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._

    def freshT(id: String) = newTermName(c.fresh(id))

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) = {
        val lenA = freshT("lenA")
        val lenB = freshT("lenB")

        q"""
        val $lenA = $inputStreamA.readSize
        val $lenB = $inputStreamB.readSize
        _root_.com.twitter.scalding.serialization.StringOrderedSerialization.binaryIntCompare($lenA,
          $inputStreamA,
          $lenB,
          $inputStreamB)
      """
      }

      override def hash(element: ctx.TermName): ctx.Tree = q"_root_.com.twitter.scalding.serialization.Hasher.string.hash($element)"

      override def put(inputStream: ctx.TermName, element: ctx.TermName) = {
        val bytes = freshT("bytes")
        val len = freshT("len")
        q"""
         val $bytes = $element.getBytes("UTF-8")
         val $len = $bytes.length
         $inputStream.writeSize($len)
          if($len > 0) {
            $inputStream.write($bytes)
          }
        """
      }
      override def get(inputStream: ctx.TermName): ctx.Tree = {
        val len = freshT("len")
        val strBytes = freshT("strBytes")
        q"""
        val $len = $inputStream.readSize
        if($len > 0) {
          val $strBytes = new Array[Byte]($len)
          $inputStream.readFully($strBytes)
          new String($strBytes, "UTF-8")
        } else {
          ""
        }
      """
      }
      override def compare(elementA: ctx.TermName, elementB: ctx.TermName) =
        q"""$elementA.compareTo($elementB)"""

      override val lazyOuterVariables: Map[String, ctx.Tree] = Map.empty
      override def length(element: Tree): CompileTimeLengthTypes[c.type] = MaybeLengthCalculation(c)(q"""
              if($element.isEmpty) {
                _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(1)
              } else {
                _root_.com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers.NoLengthCalculation
              }
            """)
    }
  }
}

