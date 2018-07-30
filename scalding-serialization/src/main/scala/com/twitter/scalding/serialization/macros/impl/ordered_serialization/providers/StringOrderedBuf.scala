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

import scala.reflect.macros.blackbox.Context

import com.twitter.scalding.serialization.macros.impl.ordered_serialization.{
  CompileTimeLengthTypes,
  TreeOrderedBuf
}
import CompileTimeLengthTypes._

object StringOrderedBuf {
  def dispatch(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    case tpe if tpe =:= c.universe.typeOf[String] => StringOrderedBuf(c)(tpe)
  }

  def apply(c: Context)(outerType: c.Type): TreeOrderedBuf[c.type] = {
    import c.universe._

    def freshT(id: String) = TermName(c.freshName(id))

    new TreeOrderedBuf[c.type] {
      override val ctx: c.type = c
      override val tpe = outerType
      override def compareBinary(inputStreamA: ctx.TermName, inputStreamB: ctx.TermName) = {
        val lenA = freshT("lenA")
        val lenB = freshT("lenB")

        q"""
        val $lenA = $inputStreamA.readPosVarInt
        val $lenB = $inputStreamB.readPosVarInt
        _root_.com.twitter.scalding.serialization.StringOrderedSerialization.binaryIntCompare($lenA,
          $inputStreamA,
          $lenB,
          $inputStreamB)
      """
      }

      override def hash(element: ctx.TermName): ctx.Tree =
        q"_root_.com.twitter.scalding.serialization.Hasher.string.hash($element)"

      override def put(inputStream: ctx.TermName, element: ctx.TermName) = {
        val bytes = freshT("bytes")
        val charLen = freshT("charLen")
        val len = freshT("len")
        q"""
         // Ascii is very common, so if the string is short,
         // we check if it is ascii:
         def isShortAscii(size: _root_.scala.Int, str: _root_.java.lang.String): _root_.scala.Boolean = (size < 65) && {
           var pos = 0
           var ascii: _root_.scala.Boolean = true
           while((pos < size) && ascii) {
             ascii = (str.charAt(pos) < 128)
             pos += 1
           }
           ascii
         }

         val $charLen = $element.length
         if ($charLen == 0) {
           $inputStream.writePosVarInt(0)
         }
         else if (isShortAscii($charLen, $element)) {
           val $bytes = new _root_.scala.Array[Byte]($charLen)
           // This deprecated gets ascii bytes out, but is incorrect
           // for non-ascii data.
           _root_.com.twitter.scalding.serialization.Undeprecated.getAsciiBytes($element, 0, $charLen, $bytes, 0)
           $inputStream.writePosVarInt($charLen)
           $inputStream.write($bytes)
         }
         else {
           // Just use utf-8
           // TODO: investigate faster ways to encode UTF-8, if
           // the bug that makes string Charsets faster than using Charset instances.
           // see for instance:
           // http://psy-lob-saw.blogspot.com/2012/12/encode-utf-8-string-to-bytebuffer-faster.html
           val $bytes = $element.getBytes("UTF-8")
           val $len = $bytes.length
           $inputStream.writePosVarInt($len)
           $inputStream.write($bytes)
         }
        """
      }
      override def get(inputStream: ctx.TermName): ctx.Tree = {
        val len = freshT("len")
        val strBytes = freshT("strBytes")
        q"""
        val $len = $inputStream.readPosVarInt
        if($len > 0) {
          val $strBytes = new _root_.scala.Array[Byte]($len)
          $inputStream.readFully($strBytes)
          new _root_.java.lang.String($strBytes, "UTF-8")
        } else {
          ""
        }
      """
      }
      override def compare(elementA: ctx.TermName, elementB: ctx.TermName) =
        q"""$elementA.compareTo($elementB)"""

      override val lazyOuterVariables: Map[String, ctx.Tree] = Map.empty
      override def length(element: Tree): CompileTimeLengthTypes[c.type] =
        MaybeLengthCalculation(c)(q"""
              if($element.isEmpty) {
                _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.DynamicLen(1)
              } else {
                _root_.com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers.NoLengthCalculation
              }
            """)
    }
  }
}
