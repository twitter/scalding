

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
package com.twitter.scalding.commons.macros.impl

import scala.language.experimental.macros
import scala.reflect.macros.Context
import org.apache.thrift.protocol.TField
import com.twitter.scrooge.ThriftStruct

import com.twitter.scalding.commons.thrift.ScroogeTProtocolOrderedSerialization
/**
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */
object ScroogeTProtocolOrderedSerializationImpl {

  def apply[T <: ThriftStruct](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[ScroogeTProtocolOrderedSerialization[T]] = {
    import c.universe._
    val tpe = T.tpe

    val companionSymbol = tpe.typeSymbol.companionSymbol

    val fieldIds: List[Tree] = companionSymbol.asModule.moduleClass.asType.toType
      .declarations
      .filter(_.name.decoded.endsWith("Field "))
      .collect{ case s: TermSymbol => s }
      .filter(_.isStatic)
      .filter(_.isVal)
      .map{ s => q"$companionSymbol.${s.getter}.id" }
      .toList

    val res = q"""
      val generatedMin: Short = List(..${fieldIds}).min
      new _root_.com.twitter.scalding.commons.thrift.ScroogeTProtocolOrderedSerialization[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
        override val minFieldId: Short = generatedMin
        override lazy val thriftStructSerializer  = $companionSymbol
      }
      """
    c.Expr[com.twitter.scalding.commons.thrift.ScroogeTProtocolOrderedSerialization[T]](res)
  }

}
