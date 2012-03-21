/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.scalding

import cascading.pipe._
import cascading.pipe.joiner._
import cascading.tuple._

import java.lang.reflect.Method

import scala.reflect.Manifest

/** Base class for classes which pack a Tuple into a serializable object.
  *
  * @author Argyris Zymnis
  * @author Oscar Boykin
  */
object TuplePacker extends LowPriorityTuplePackers
abstract class TuplePacker[T] extends java.io.Serializable {
  def newInstance(input : TupleEntry) : T
}

trait LowPriorityTuplePackers extends TupleConversions {
  implicit def genericTuplePacker[T : Manifest] = new ReflectionTuplePacker[T]
}

/** Packs a tuple into any object with set methods, e.g. thrift or proto objects.
  * TODO: verify that protobuf setters for field camel_name are of the form setCamelName.
  * In that case this code works for proto.
  *
  * @author Argyris Zymnis
  * @author Oscar Boykin
  */
class ReflectionTuplePacker[T](implicit m : Manifest[T]) extends TuplePacker[T] {

  def lowerFirst(s : String) = s.substring(0,1).toLowerCase + s.substring(1)
  // Cut out "set" and lower case the first after
  def setterToFieldName(setter : Method) = lowerFirst(setter.getName.substring(3))

  // Do all the reflection for the setters we need:
  // This needs to be lazy because Method is not serializable
  // TODO: filter by isAccessible, which somehow seems to fail
  lazy val setters = m.erasure
    .getDeclaredMethods
    .filter { _.getName.startsWith("set") }
    .groupBy { setterToFieldName(_) }
    .mapValues { _.head }

  override def newInstance(input : TupleEntry) : T = {
    val newInst = m.erasure.newInstance()
    val fields = input.getFields
    (0 until fields.size).map { idx =>
      val thisField = fields.get(idx)
      val setMethod = setters(thisField.toString)
      setMethod.invoke(newInst, input.getObject(thisField))
    }
    newInst.asInstanceOf[T]
  }
}
