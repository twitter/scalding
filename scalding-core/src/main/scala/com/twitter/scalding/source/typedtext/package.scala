package com.twitter.scalding.source

import com.twitter.scalding.TypeDescriptor
import com.twitter.scalding.macros.impl.TypeDescriptorProviderImpl
import scala.language.experimental.macros

package object typedtext {
  /**
   * Import this to get a TypeDescriptor for a case class
   */
  implicit def descriptor[T]: TypeDescriptor[T] = macro TypeDescriptorProviderImpl.caseClassTypeDescriptorImpl[T]
}
