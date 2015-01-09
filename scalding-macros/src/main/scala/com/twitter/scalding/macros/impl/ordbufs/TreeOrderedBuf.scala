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
package com.twitter.scalding.macros.impl.ordbufs

import scala.reflect.macros.Context
import scala.language.experimental.macros

import com.twitter.scalding._
import java.nio.ByteBuffer
import com.twitter.scalding.typed.OrderedBufferable

object TreeOrderedBuf {
  def toOrderedBufferable[T](c: Context)(t: TreeOrderedBuf[c.type])(implicit T: t.ctx.WeakTypeTag[T]): t.ctx.Expr[OrderedBufferable[T]] = {
    import t.ctx.universe._
    t.ctx.Expr[OrderedBufferable[T]](q"""
      new _root_.com.twitter.scalding.typed.OrderedBufferable[$T] {

        private[this] def innerCompare(a: _root_.java.nio.ByteBuffer, b: _root_.java.nio.ByteBuffer): Int = {
            val ${t.compareBinary._1} = a
            val ${t.compareBinary._2} = b
            ${t.compareBinary._3}
            return 0
          }

        def compareBinary(a: _root_.java.nio.ByteBuffer, b: _root_.java.nio.ByteBuffer): _root_.com.twitter.scalding.typed.OrderedBufferable.Result = {
          try {
             val r = innerCompare(a, b)
             if (r < 0) {
                _root_.com.twitter.scalding.typed.OrderedBufferable.Less
              } else if (r > 0) {
                _root_.com.twitter.scalding.typed.OrderedBufferable.Greater
              } else {
                _root_.com.twitter.scalding.typed.OrderedBufferable.Equal
              }
            }
            catch { case _root_.scala.util.control.NonFatal(e) =>
              _root_.com.twitter.scalding.typed.OrderedBufferable.CompareFailure(e)
            }
          }

        def hash(passedInObjectToHash: $T): Int = {
          val ${t.hash._1} = passedInObjectToHash
          ${t.hash._2}
        }

        def get(from: _root_.java.nio.ByteBuffer): _root_.scala.util.Try[(_root_.java.nio.ByteBuffer, $T)] = {
          val ${t.get._1} = from.duplicate
          try {
             _root_.scala.util.Success((${t.get._1}, ${t.get._2}))
          } catch { case _root_.scala.util.control.NonFatal(e) =>
            _root_.scala.util.Failure(e)
          }
        }

        def put(into: _root_.java.nio.ByteBuffer, t: $T): _root_.java.nio.ByteBuffer =  {
          val ${t.put._1} = into.duplicate
          val ${t.put._2} = t
          ${t.put._3}
          ${t.put._1}
        }

        private def innerMemCompare(x: $T, y: $T): Int = {
          val ${t.compare._1} = x
          val ${t.compare._2} = y
          ${t.compare._3}
          return 0
        }

         def compare(x: $T, y: $T): Int = {
          val tmp = innerMemCompare(x, y)
          if(tmp < 0) return -1
          if(tmp > 0) return 1
          return 0
        }
      }
    """)
  }
}
abstract class TreeOrderedBuf[C <: Context] {
  val ctx: C
  val tpe: ctx.Type
  // Expected byte buffers to be in values a and b respestively, the tree has the value of the result
  def compareBinary: (ctx.TermName, ctx.TermName, ctx.Tree) // ctx.Expr[Function2[ByteBuffer, ByteBuffer, Int]]
  // expects the thing to be tested on in the indiciated TermName
  def hash: (ctx.TermName, ctx.Tree)

  // Place input in param 1, tree to return result in param 2
  def get: (ctx.TermName, ctx.Tree)

  // BB input in param 1
  // Other input of type T in param 2
  def put: (ctx.TermName, ctx.TermName, ctx.Tree)

  def compare: (ctx.TermName, ctx.TermName, ctx.Tree)

  override def toString = {
    s"""
    |TreeOrderedBuf {
    |
    |compareBinary: $compareBinary
    |
    |hash: $hash
    |
    |}
    """.stripMargin('|')
  }
}
