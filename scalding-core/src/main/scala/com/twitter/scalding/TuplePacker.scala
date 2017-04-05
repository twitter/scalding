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

import cascading.tuple._

import java.lang.reflect.Method
import java.lang.reflect.Constructor

import scala.reflect.Manifest

/**
 * Typeclass for packing a cascading Tuple into some type T,
 * this is used to put fields of a cascading tuple into Thrift, Protobuf,
 * or case classes, for instance, but you can add your own instances to control
 * how this is done.
 *
 * @author Argyris Zymnis
 * @author Oscar Boykin
 */
trait TuplePacker[T] extends java.io.Serializable {
  def newConverter(fields: Fields): TupleConverter[T]
}

object TuplePacker extends CaseClassPackers

trait CaseClassPackers extends LowPriorityTuplePackers {
  implicit def caseClassPacker[T <: Product](implicit mf: Manifest[T]): OrderedTuplePacker[T] = new OrderedTuplePacker[T]
}

trait LowPriorityTuplePackers extends java.io.Serializable {
  implicit def genericTuplePacker[T: Manifest]: ReflectionTuplePacker[T] = new ReflectionTuplePacker[T]
}

/**
 * Packs a tuple into any object with set methods, e.g. thrift or proto objects.
 * TODO: verify that protobuf setters for field camel_name are of the form setCamelName.
 * In that case this code works for proto.
 *
 * @author Argyris Zymnis
 * @author Oscar Boykin
 */
class ReflectionTuplePacker[T](implicit m: Manifest[T]) extends TuplePacker[T] {
  override def newConverter(fields: Fields) = new ReflectionTupleConverter[T](fields)(m)
}

class ReflectionTupleConverter[T](fields: Fields)(implicit m: Manifest[T]) extends TupleConverter[T] {
  override val arity = fields.size

  def lowerFirst(s: String) = s.substring(0, 1).toLowerCase + s.substring(1)
  // Cut out "set" and lower case the first after
  def setterToFieldName(setter: Method) = lowerFirst(setter.getName.substring(3))

  /* The `_.get` is safe because of the `_.isEmpty` check.  ScalaTest does not
   * seem to support a more type safe way of doing this.
   */
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def validate(): Unit = {
    //We can't touch setters because that shouldn't be accessed until map/reduce side, not
    //on submitter.
    val missing = Dsl.asList(fields).find { f => !getSetters.contains(f.toString) }

    assert(missing.isEmpty, "Field: " + missing.get.toString + " not in setters")
  }
  validate()

  def getSetters = m.runtimeClass
    .getDeclaredMethods
    .filter { _.getName.startsWith("set") }
    .groupBy { setterToFieldName(_) }
    .mapValues { _.head }

  // Do all the reflection for the setters we need:
  // This needs to be lazy because Method is not serializable
  // TODO: filter by isAccessible, which somehow seems to fail
  lazy val setters = getSetters

  override def apply(input: TupleEntry): T = {
    val newInst = m.runtimeClass.newInstance().asInstanceOf[T]
    val fields = input.getFields
    (0 until fields.size).map { idx =>
      val thisField = fields.get(idx)
      val setMethod = setters(thisField.toString)
      setMethod.invoke(newInst, input.getObject(thisField))
    }
    newInst
  }
}

/**
 * This just blindly uses the first public constructor with the same arity as the fields size
 */
class OrderedTuplePacker[T](implicit m: Manifest[T]) extends TuplePacker[T] {
  override def newConverter(fields: Fields) = new OrderedConstructorConverter[T](fields)(m)
}

class OrderedConstructorConverter[T](fields: Fields)(implicit mf: Manifest[T]) extends TupleConverter[T] {
  override val arity = fields.size
  // Keep this as a method, so we can validate by calling, but don't serialize it, and keep it lazy
  // below
  def getConstructor = mf.runtimeClass
    .getConstructors
    .filter { _.getParameterTypes.size == fields.size }
    .head.asInstanceOf[Constructor[T]]

  //Make sure we can actually get a constructor:
  getConstructor

  lazy val cons = getConstructor

  override def apply(input: TupleEntry): T = {
    val tup = input.getTuple
    val args = (0 until tup.size).map { tup.getObject(_) }
    cons.newInstance(args: _*)
  }
}
