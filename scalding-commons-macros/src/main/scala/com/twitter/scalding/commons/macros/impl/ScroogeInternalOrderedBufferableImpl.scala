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
import scala.util.Random

import com.twitter.scalding.typed.OrderedBufferable
import com.twitter.scalding.macros.impl.ordbufs._
import com.twitter.scalding.commons.macros.impl.ordbufs._
import com.twitter.scalding.macros.impl.OrderedBufferableProviderImpl

object ScroogeInternalOrderedBufferableImpl {
  private def dispatcher(c: Context): PartialFunction[c.Type, TreeOrderedBuf[c.type]] = {
    import c.universe._
    def buildDispatcher: PartialFunction[c.Type, TreeOrderedBuf[c.type]] = ScroogeInternalOrderedBufferableImpl.dispatcher(c)
    val scroogeDispatcher = ScroogeOrderedBuf.dispatch(c)(buildDispatcher)

    scroogeDispatcher.orElse(OrderedBufferableProviderImpl.scaldingBasicDispatchers(c)(buildDispatcher)).orElse {
      case tpe: Type => c.abort(c.enclosingPosition, s"""Unable to find OrderedBufferable for type ${tpe}""")
    }
  }

  def apply[T](c: Context)(implicit T: c.WeakTypeTag[T]): c.Expr[OrderedBufferable[T]] = {
    import c.universe._

    val b: TreeOrderedBuf[c.type] = dispatcher(c)(T.tpe)
    TreeOrderedBuf.toOrderedBufferable[T](c)(b)
  }
}
