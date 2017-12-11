package com.twitter.scalding

import org.scalatest.MustMatchers
import org.scalatest.FreeSpec

package object quotation {
  def typeName[T](implicit ct: reflect.ClassTag[T]) = TypeName(ct.runtimeClass.getName)
  trait Test extends FreeSpec with MustMatchers
}
