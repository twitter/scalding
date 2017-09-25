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
package com.twitter.scalding.serialization.macros.impl.ordered_serialization.providers

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

import com.twitter.scalding._
import com.twitter.scalding.serialization.macros.impl.ordered_serialization.{
  CompileTimeLengthTypes,
  ProductLike,
  TreeOrderedBuf
}
import CompileTimeLengthTypes._

import java.nio.ByteBuffer
import com.twitter.scalding.serialization.OrderedSerialization

object ByteBufferOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe =:= c.universe.typeOf[ByteBuffer] => ByteBufferOrderedBuf(c)(tpe)
  }

  def apply(c: Context)(outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._

    def freshT(id: String) = TermName(c.freshName(id))

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def hash(element: ctx.TermName): ctx.Tree = q"$element.hashCode"

      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) = {
        val lenA = freshT("lenA")
        val lenB = freshT("lenB")
        val queryLength = freshT("queryLength")
        val incr = freshT("incr")
        val state = freshT("state")
        q"""
      val $lenA: Int = $inputStreamA.readPosVarInt
      val $lenB: Int = $inputStreamB.readPosVarInt

      val $queryLength = _root_.scala.math.min($lenA, $lenB)
      var $incr = 0
      var $state = 0

      while($incr < $queryLength && $state == 0) {
        $state = _root_.java.lang.Byte.compare($inputStreamA.readByte, $inputStreamB.readByte)
        $incr = $incr + 1
      }
      if($state == 0) {
        _root_.java.lang.Integer.compare($lenA, $lenB)
      } else {
        $state
      }
      """
      }
      override def put(inputStream: ctx.TermName, element: ctx.TermName) =
        q"""
      $inputStream.writePosVarInt($element.remaining)
      $inputStream.writeBytes($element.array, $element.arrayOffset + $element.position, $element.remaining)
      """

      override def get(inputStream: ctx.TermName): ctx.Tree = {
        val lenA = freshT("lenA")
        val bytes = freshT("bytes")
        q"""
      val $lenA = $inputStream.readPosVarInt
      val $bytes = new Array[Byte]($lenA)
      $inputStream.readFully($bytes)
      _root_.java.nio.ByteBuffer.wrap($bytes)
    """
      }
      override def compare(elementA: ctx.TermName, elementB: ctx.TermName): ctx.Tree = q"""
        $elementA.compareTo($elementB)
      """
      override def length(element: Tree): CompileTimeLengthTypes[c.type] = {
        val tmpLen = freshT("tmpLen")
        FastLengthCalculation(c)(q"""
          val $tmpLen = $element.remaining
          posVarIntSize($tmpLen) + $tmpLen
        """)
      }

      def lazyOuterVariables: Map[String, ctx.Tree] = Map.empty
    }
  }
}
