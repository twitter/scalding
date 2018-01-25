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

import scala.reflect.Manifest

/**
 * Typeclass for objects which unpack an object into a tuple.
 * The packer can verify the arity, types, and also the existence
 * of the getter methods at plan time, without having the job
 * blow up in the middle of a run.
 *
 * @author Argyris Zymnis
 * @author Oscar Boykin
 */
object TupleUnpacker extends LowPriorityTupleUnpackers
trait TupleUnpacker[T] extends java.io.Serializable {
  def newSetter(fields: Fields): TupleSetter[T]
  def getResultFields(fields: Fields): Fields = fields
}

trait LowPriorityTupleUnpackers {
  implicit def genericUnpacker[T: Manifest]: ReflectionTupleUnpacker[T] = new ReflectionTupleUnpacker[T]
}

/**
 * A helper for working with class reflection.
 * Allows us to avoid code repetition.
 */
object ReflectionUtils {

  /**
   * Returns the set of fields in the given class.
   * We use a List to ensure fields are in the same
   * order they were declared.
   */
  def fieldsOf[T](c: Class[T]): List[String] =
    c.getDeclaredFields
      .map { f => f.getName }
      .toList
      .distinct

  /**
   * For a given class, give a function that takes
   * a T, and a fieldname and returns the values.
   */
  // def fieldGetters[T](c: Class[T]): (T,String) => AnyRef

  /**
   * For a given class, give a function of T, fieldName,
   * fieldValue that returns a new T (possibly a copy,
   * if T is immutable).
   */
  // def fieldSetters[T](c: Class[T]): (T,String,AnyRef) => T
}

class ReflectionTupleUnpacker[T](implicit m: Manifest[T]) extends TupleUnpacker[T] {

  // A Fields object representing all of m's
  // fields, in the declared field order.
  // Lazy because we need this twice or not at all.
  lazy val allFields = new Fields(ReflectionUtils.fieldsOf(m.runtimeClass).toSeq: _*)

  /**
   * A helper to check the passed-in
   * fields to see if Fields.ALL is set.
   * If it is, return lazy allFields.
   */
  def expandIfAll(fields: Fields) =
    if (fields.isAll) allFields else fields

  override def newSetter(fields: Fields) =
    new ReflectionSetter[T](expandIfAll(fields))(m)

  override def getResultFields(fields: Fields): Fields =
    expandIfAll(fields)
}

class ReflectionSetter[T](fields: Fields)(implicit m: Manifest[T]) extends TupleSetter[T] {

  validate // Call the validation method at the submitter

  // This is lazy because it is not serializable
  // Contains a list of methods used to set the Tuple from an input of type T
  lazy val setters = makeSetters

  // Methods and Fields are not serializable so we
  // make these defs instead of vals
  // TODO: filter by isAccessible, which somehow seems to fail
  def methodMap = m.runtimeClass
    .getDeclaredMethods
    // Keep only methods with 0 parameter types
    .filter { m => m.getParameterTypes.length == 0 }
    .groupBy { _.getName }
    .mapValues { _.head }

  // TODO: filter by isAccessible, which somehow seems to fail
  def fieldMap = m.runtimeClass
    .getDeclaredFields
    .groupBy { _.getName }
    .mapValues { _.head }

  def makeSetters = {
    (0 until fields.size).map { idx =>
      val fieldName = fields.get(idx).toString
      setterForFieldName(fieldName)
    }
  }

  // This validation makes sure that the setters exist
  // but does not save them in a val (due to serialization issues)
  def validate = makeSetters

  override def apply(input: T): Tuple = {
    val values = setters.map { setFn => setFn(input) }
    new Tuple(values: _*)
  }

  override def arity = fields.size

  private def setterForFieldName(fieldName: String): (T => AnyRef) = {
    getValueFromMethod(createGetter(fieldName))
      .orElse(getValueFromMethod(fieldName))
      .orElse(getValueFromField(fieldName))
      .getOrElse(
        throw new TupleUnpackerException("Unrecognized field: " + fieldName + " for class: " + m.runtimeClass.getName))
  }

  private def getValueFromField(fieldName: String): Option[(T => AnyRef)] = {
    fieldMap.get(fieldName).map { f => (x: T) => f.get(x) }
  }

  private def getValueFromMethod(methodName: String): Option[(T => AnyRef)] = {
    methodMap.get(methodName).map { m => (x: T) => m.invoke(x) }
  }

  private def upperFirst(s: String) = s.substring(0, 1).toUpperCase + s.substring(1)
  private def createGetter(s: String) = "get" + upperFirst(s)
}

class TupleUnpackerException(args: String) extends Exception(args)
