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

import java.io.Serializable
import java.lang.reflect.Type

import cascading.tuple.Fields

/**
 * Trait to assist with creating objects such as [[TypedTsv]] to read from separated files.
 * Override separator, skipHeader, writeHeader as needed.
 */
trait TypedSeperatedFile extends Serializable {
  def separator: String
  def skipHeader: Boolean = false
  def writeHeader: Boolean = false

  def apply[T: Manifest: TupleConverter: TupleSetter](path: String): FixedPathTypedDelimited[T] =
    apply(Seq(path))

  def apply[T: Manifest: TupleConverter: TupleSetter](paths: Seq[String]): FixedPathTypedDelimited[T] = {
    val f = Dsl.intFields(0 until implicitly[TupleConverter[T]].arity)
    apply(paths, f)
  }

  def apply[T: Manifest: TupleConverter: TupleSetter](path: String, f: Fields): FixedPathTypedDelimited[T] =
    apply(Seq(path), f)

  def apply[T: Manifest: TupleConverter: TupleSetter](paths: Seq[String], f: Fields): FixedPathTypedDelimited[T] =
    new FixedPathTypedDelimited[T](paths, f, skipHeader, writeHeader, separator)
}

/**
 * Typed tab separated values file
 */
object TypedTsv extends TypedSeperatedFile {
  val separator = "\t"
}

/**
 * Typed comma separated values file
 */
object TypedCsv extends TypedSeperatedFile {
  val separator = ","
}

/**
 * Typed pipe separated values flile
 */
object TypedPsv extends TypedSeperatedFile {
  val separator = "|"
}

/**
 * Typed one separated values file (commonly used by Pig)
 */
object TypedOsv extends TypedSeperatedFile {
  val separator = "\1"
}

object FixedPathTypedDelimited {
  def apply[T: Manifest: TupleConverter: TupleSetter](path: String, separator: String): FixedPathTypedDelimited[T] =
    apply(Seq(path), separator)

  def apply[T: Manifest: TupleConverter: TupleSetter](paths: Seq[String], separator: String): FixedPathTypedDelimited[T] = {
    val f = Dsl.intFields(0 until implicitly[TupleConverter[T]].arity)
    apply(paths, f, separator)
  }

  def apply[T: Manifest: TupleConverter: TupleSetter](path: String, f: Fields, separator: String): FixedPathTypedDelimited[T] =
    apply(Seq(path), f, separator)

  def apply[T: Manifest: TupleConverter: TupleSetter](paths: Seq[String], f: Fields, separator: String): FixedPathTypedDelimited[T] =
    new FixedPathTypedDelimited[T](paths, f, false, false, separator)
}

/**
 * Allows you to set the types, prefer this:
 * If T is a subclass of Product, we assume it is a tuple. If it is not, wrap T in a Tuple1:
 * e.g. TypedTsv[Tuple1[List[Int]]]
 */
trait TypedDelimited[T] extends DelimitedScheme
  with Mappable[T] with TypedSink[T] {

  override val skipHeader: Boolean = false
  override val writeHeader: Boolean = false
  override val separator: String = "\t"

  implicit val mf: Manifest[T]
  implicit val conv: TupleConverter[T]
  implicit val tset: TupleSetter[T]

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](conv)
  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](tset)

  override val types: Array[Class[_]] =
    if (classOf[scala.Product].isAssignableFrom(mf.erasure)) {
      //Assume this is a Tuple:
      mf.typeArguments.map { _.erasure }.toArray
    } else {
      //Assume there is only a single item
      Array(mf.erasure)
    }

  // This is used to add types to a Field, which Cascading now supports. While we do not do this much generally
  // through the code, it is good practice and something that, ideally, we can do wherever possible.
  def addTypes(sel: Array[Comparable[_]]) = new Fields(sel, types.map(_.asInstanceOf[Type]))

  override val fields: Fields = addTypes((0 until types.length).toArray.map(_.asInstanceOf[Comparable[_]]))
  final override def sinkFields = fields
}

class FixedPathTypedDelimited[T](p: Seq[String],
  override val fields: Fields = Fields.ALL,
  override val skipHeader: Boolean = false,
  override val writeHeader: Boolean = false,
  override val separator: String = "\t")(implicit override val mf: Manifest[T], override val conv: TupleConverter[T],
    override val tset: TupleSetter[T]) extends FixedPathSource(p: _*)
  with TypedDelimited[T] {

  override lazy val toString: String = "FixedPathTypedDelimited" +
    ((p, fields, skipHeader, writeHeader, separator, mf).toString)

  override def equals(that: Any): Boolean = Option(that)
    .map { _.toString == this.toString }.getOrElse(false)

  override lazy val hashCode: Int = toString.hashCode
}
