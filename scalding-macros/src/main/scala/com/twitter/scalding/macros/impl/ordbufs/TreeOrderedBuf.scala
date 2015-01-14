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

  def lengthEncodingSize(c: Context)(len: c.universe.Tree) = {
    import c.universe._
    q"""
      if($len < 255) {
        1
      } else if($len < 65535) {
        3
      } else {
        7
      }
      """
  }
  def injectWriteListSize(c: Context)(len: c.universe.TermName, bb: c.universe.TermName) = {
    import c.universe._
    injectWriteListSizeTree(c)(q"$len", bb)
  }

  def injectWriteListSizeTree(c: Context)(len: c.universe.Tree, bb: c.universe.TermName) = {
    import c.universe._
    q"""
         if ($len < 255) {
          $bb.put($len.toByte)
         } else if($len < 65535) {
          $bb.put(-1:Byte)
          $bb.putShort($len.toShort)
         } else {
          $bb.put(-1:Byte)
          $bb.putShort(-1:Short)
          $bb.putInt($len)
        }"""
  }

  def injectReadListSize(c: Context)(bb: c.universe.TermName) = {
    import c.universe._
    def freshT(id: String) = newTermName(c.fresh(s"fresh_$id"))

    val initialB = freshT("byteTry")
    val initialShort = freshT("shortTry")
    q"""
        val $initialB = $bb.get
        if ($initialB == (-1: Byte)) {
          val $initialShort: Short = $bb.getShort
          if($initialShort == (-1: Short)) {
            $bb.getInt
          } else {
            if ($initialShort < 0) {
            $initialShort.toInt + 65536
            } else {
              $initialShort.toInt
            }
          }
        } else {
          if ($initialB < 0) {
            $initialB.toInt + 256
          } else {
            $initialB.toInt
          }
        }
      """
  }

  def toOrderedBufferable[T](c: Context)(t: TreeOrderedBuf[c.type])(implicit T: t.ctx.WeakTypeTag[T]): t.ctx.Expr[OrderedBufferable[T]] = {
    import t.ctx.universe._
    def freshT(id: String) = newTermName(c.fresh(s"fresh_$id"))
    val outputLength = freshT("outputLength")

    def getLength(target: TermName) = {
      t.length(q"$target") match {
        case Left(t) => q"t"
        case Right(s) => s
      }
    }

    def writeLength(bb: TermName, target: TermName) = {
      t.length(q"$target") match {
        case Left(t) => q""
        case Right(d) => q"""
        val $outputLength = ${d}
        ${injectWriteListSize(c)(outputLength, bb)}
        """
      }
    }

    def readLength(bb: TermName) = {
      t.length(q"e") match {
        case Left(t) => q"$t"
        case Right(d) => q"""
          ${injectReadListSize(c)(bb)}
          """
      }
    }

    val lazyVariables = t.lazyOuterVariables.map {
      case (n, t) =>
        val termName = newTermName(n)
        q"""lazy val $termName = $t"""
    }

    t.ctx.Expr[OrderedBufferable[T]](q"""
      new _root_.com.twitter.scalding.typed.OrderedBufferable[$T] with _root_.com.twitter.bijection.macros.MacroGenerated  {
        ..$lazyVariables

        def compareBinary(a: _root_.java.nio.ByteBuffer, b: _root_.java.nio.ByteBuffer): _root_.com.twitter.scalding.typed.OrderedBufferable.Result = {
          try {
            val ${t.compareBinary._1} = a
            val ${t.compareBinary._2} = b

            val lenA = ${readLength(t.compareBinary._1)}
            val dataStartA = ${t.compareBinary._1}.position

            val lenB = ${readLength(t.compareBinary._2)}
            val dataStartB = ${t.compareBinary._2}.position

            val subSetRes = if((lenA == lenB) && lenA > 24) {
              val subSetA = ${t.compareBinary._1}.slice
              subSetA.limit(lenA)
              val subSetB = ${t.compareBinary._2}.slice
              subSetB.limit(lenB)
              subSetA.compareTo(subSetB)
            } else {
              -1
            }

             val r = if(subSetRes == 0) {
              0
              } else {
              ${t.compareBinary._3}
            }
             ${t.compareBinary._1}.position(dataStartA + lenA)
             ${t.compareBinary._2}.position(dataStartB + lenB)

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

        final def binaryLength(element: $T): Int = {
          ${getLength(newTermName("element"))}
        }

        def get(from: _root_.java.nio.ByteBuffer): _root_.scala.util.Try[(_root_.java.nio.ByteBuffer, $T)] = {
          val ${t.get._1} = from.duplicate
          try {
              val $outputLength = ${readLength(t.get._1)}
             _root_.scala.util.Success((${t.get._1}, ${t.get._2}))
          } catch { case _root_.scala.util.control.NonFatal(e) =>
            _root_.scala.util.Failure(e)
          }
        }

        def put(into: _root_.java.nio.ByteBuffer, e: $T): _root_.java.nio.ByteBuffer =  {
          val ${t.put._1} = into.duplicate
          val ${t.put._2} = e

          ${writeLength(t.put._1, t.put._2)}

          ${t.put._3}
          ${t.put._1}
        }

        def compare(x: $T, y: $T): Int = {
          val ${t.compare._1} = x
          val ${t.compare._2} = y
          ${t.compare._3}
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

  def lazyOuterVariables: Map[String, ctx.Tree]
  // Return the constant size or a tree
  def length(element: ctx.universe.Tree): Either[Int, ctx.Tree]

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
