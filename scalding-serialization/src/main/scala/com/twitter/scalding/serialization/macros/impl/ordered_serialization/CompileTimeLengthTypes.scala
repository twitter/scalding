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
package com.twitter.scalding.serialization.macros.impl.ordered_serialization

import scala.language.experimental.macros
import scala.reflect.macros.Context

sealed trait CompileTimeLengthTypes[C <: Context] {
  val ctx: C
  def t: ctx.Tree
}
object CompileTimeLengthTypes {

  // Repesents an Int returning
  object FastLengthCalculation {
    def apply(c: Context)(tree: c.Tree): FastLengthCalculation[c.type] =
      new FastLengthCalculation[c.type] {
        override val ctx: c.type = c
        override val t: c.Tree = tree
      }
  }

  trait FastLengthCalculation[C <: Context] extends CompileTimeLengthTypes[C]

  object MaybeLengthCalculation {
    def apply(c: Context)(tree: c.Tree): MaybeLengthCalculation[c.type] =
      new MaybeLengthCalculation[c.type] {
        override val ctx: c.type = c
        override val t: c.Tree = tree
      }
  }

  trait MaybeLengthCalculation[C <: Context] extends CompileTimeLengthTypes[C]

  object ConstantLengthCalculation {
    def apply(c: Context)(intArg: Int): ConstantLengthCalculation[c.type] =
      new ConstantLengthCalculation[c.type] {
        override val toInt = intArg
        override val ctx: c.type = c
        override val t: c.Tree = {
          import c.universe._
          q"$intArg"
        }
      }
  }

  trait ConstantLengthCalculation[C <: Context] extends CompileTimeLengthTypes[C] {
    def toInt: Int
  }

  object NoLengthCalculationAvailable {
    def apply(c: Context): NoLengthCalculationAvailable[c.type] = {
      new NoLengthCalculationAvailable[c.type] {
        override val ctx: c.type = c
        override def t = {
          import c.universe._
          q"""_root_.scala.sys.error("no length available")"""
        }
      }
    }
  }

  trait NoLengthCalculationAvailable[C <: Context] extends CompileTimeLengthTypes[C]
}
