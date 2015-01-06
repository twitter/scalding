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
import org.apache.thrift.TBase
import org.apache.thrift.protocol.TField

import com.twitter.scalding.commons.thrift.TBaseOrderedBufferable
/**
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */
object TBaseOrderedBufferableImpl {

  def getTFields(clazz: Class[_]): List[org.apache.thrift.protocol.TField] = {
    clazz.getDeclaredFields.toList.filter { f =>
      java.lang.reflect.Modifier.isStatic(f.getModifiers)
    }.filter { f =>
      f.getType == classOf[org.apache.thrift.protocol.TField]
    }.map { f =>
      f.setAccessible(true)
      f.get(null).asInstanceOf[TField]
    }
  }

  def getMinField(clazz: Class[_]): Short =
    getTFields(clazz).map(_.id).min

  def apply[T <: TBase[_, _]](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[TBaseOrderedBufferable[T]] = {
    import c.universe._
    val tpe = T.tpe
    val res = q"""
      new _root_.com.twitter.scalding.commons.thrift.TBaseOrderedBufferable[$T] with _root_.com.twitter.bijection.macros.MacroGenerated {
        lazy val generatedMin = _root_.com.twitter.scalding.commons.macros.impl.TBaseOrderedBufferableImpl.getMinField(classOf[$T])
        override val minFieldId: Short = generatedMin
      }
      """

    c.Expr[com.twitter.scalding.commons.thrift.TBaseOrderedBufferable[T]](res)
  }

}
