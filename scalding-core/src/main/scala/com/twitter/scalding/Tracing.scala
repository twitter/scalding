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

import java.lang.reflect.InvocationTargetException

import org.slf4j.{ Logger, LoggerFactory => LogManager }

/**
 * Calling init registers "com.twitter.scalding" as a "tracing boundary" for
 * Cascading. That means that when Cascading sends trace information to
 * a DocumentService such as Driven, the trace will have information about
 * the caller of Scalding instead of about the internals of Scalding.
 * com.twitter.scalding.Job and its subclasses will automatically
 * initialize Tracing.
 *
 * register and unregister methods are provided for testing, but
 * should not be needed for most development
 */
object Tracing {
  private val LOG: Logger = LogManager.getLogger(this.getClass)

  // TODO: remove this once we no longer want backwards compatiblity
  // with cascading versions pre 2.6
  private val traceUtilClassName = "cascading.util.TraceUtil"

  /**
   * Put a barrier at com.twitter.scalding, but exclude things like Tool
   * that are common entry points for calling user code
   */
  private val defaultRegex = """^com\.twitter\.scalding\.(?!Tool|Job|ExecutionContext).*"""

  register()

  /**
   * Forces the initialization of the Tracing object which in turn causes
   * the one time registration of "com.twitter.scalding" as a
   * tracing boundary in Cascading
   */
  def init(): Unit = { /* do nothing */ }

  /**
   * Explicitly registers "com.twitter.scalding" as a Cascading
   * tracing boundary. Normally not needed, but may be useful
   * after a call to unregister()
   */
  def register(regex: String = defaultRegex) = invokeStaticMethod(traceUtilClassName, "registerApiBoundary", regex)

  /**
   * Unregisters "com.twitter.scalding" as a Cascading
   * tracing bounardy. After calling this, Cascading DocumentServices
   * such as Driven will show nodes as being created by Scalding
   * class such as RichPipe instead of end user written code. This
   * should normally not be called but can be useful in testing
   * the development of Scalding internals
   */
  def unregister(regex: String = defaultRegex) = invokeStaticMethod(traceUtilClassName, "unregisterApiBoundary", regex)

  /**
   * Use reflection to register/unregister tracing boundaries so that cascading versions prior to 2.6 can be used
   * without completely breaking
   */
  private def invokeStaticMethod(clazz: String, methodName: String, args: AnyRef*): Unit = {
    try {
      val argTypes = args map (_.getClass())
      Class.forName(clazz).getMethod(methodName, argTypes: _*).invoke(null, args: _*)
    } catch {
      case e @ (_: NoSuchMethodException |
        _: SecurityException |
        _: IllegalAccessException |
        _: IllegalArgumentException |
        _: InvocationTargetException |
        _: NullPointerException |
        _: ClassNotFoundException) => LOG.warn("There was an error initializing tracing. " +
        "Tracing information in DocumentServices such as Driven may point to Scalding code instead of " +
        "user code. The most likely cause is a mismatch in Cascading library version. Upgrading the " +
        "Cascading library to at least 2.6 should fix this issue.The cause was [" + e + "]")
    }
  }
}