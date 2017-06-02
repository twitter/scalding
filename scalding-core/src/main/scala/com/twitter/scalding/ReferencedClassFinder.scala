package com.twitter.scalding

import com.twitter.scalding.typed.CoGroupable
import org.slf4j.LoggerFactory
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{ NullaryMethodType, RuntimeMirror, Symbol, Type, TypeRef }

object ReferencedClassFinder {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  private val baseContainers = List(
    classOf[Execution[_]],
    classOf[TypedPipe[_]],
    classOf[TypedSink[_]],
    classOf[TypedSource[_]],
    classOf[CoGroupable[_, _]],
    classOf[KeyedList[_, _]])

  /**
   * Add the given type, as well as all referenced types to the cascading tokens list.
   * note, for maximal efficiency, you should also register those types with the kryo
   * instantiator being used.
   */
  def addCascadingTokensFrom(c: Class[_], config: Config): Config = {
    CascadingTokenUpdater.update(config, findReferencedClasses(c) + c)
  }

  /**
   * Reflect over a scalding job to try and identify types it uses so they can be tokenized by cascading.
   * Since scala reflection is broken with the Hadoop InterfaceAudiance annotation (see
   * https://issues.scala-lang.org/browse/SI-10129), we can't iterate over scalaType.members, so we instead use java
   * reflection to iterate over fields to find the ones we care about, and then look those up in scala reflection to
   * find the full un-erased type signatures, and try to find types from those.
   *
   * Note: this not guaranteed to find every used type. Eg, it can't find types used in a step that isn't
   * referred to in a field
   */
  def findReferencedClasses(outerClass: Class[_]): Set[Class[_]] = {
    val scalaPackage = Package.getPackage("scala")
    val mirror = universe.runtimeMirror(outerClass.getClassLoader)
    getClassType(outerClass, mirror) match {
      case Some(scalaType) =>
        (for {
          field <- outerClass.getDeclaredFields
          if baseContainers.exists(_.isAssignableFrom(field.getType))
          scalaSignature = scalaType.member(universe.stringToTermName(field.getName)).typeSignature
          clazz <- getClassesForType(mirror, scalaSignature)
          /* The scala root package contains a lot of shady stuff, eg compile-time wrappers (scala.Int/Array etc),
           * which reflection will present as type parameters. Skip the whole package - chill-hadoop already ensures most
           * of the ones we care about (eg tuples) get tokenized in cascading.
           */
          if !(clazz.isPrimitive || clazz.isArray || clazz.getPackage.equals(scalaPackage))
        } yield {
          clazz
        }).toSet
      case _ => Set()
    }
  }

  private def getClassType(outerClass: Class[_], mirror: universe.Mirror): Option[universe.Type] = {
    try {
      Some(mirror.classSymbol(outerClass).toType)
    } catch {
      // In some cases we fail to find references classes, it shouldn't be fatal.
      case r: RuntimeException if r.getMessage.contains("error reading Scala signature") =>
        LOG.warn(s"Unable to find referenced classes for: $outerClass. This is potentially due to missing dependencies", r)
        None
      case t: Throwable if t.getMessage.contains("illegal cyclic reference") =>
        // Related to: https://issues.scala-lang.org/browse/SI-10129
        LOG.warn(s"Unable to find referenced classes for: $outerClass. Related to Scala language issue: SI-10129", t)
        None
      case ae: AssertionError if ae.getMessage.contains("no symbol could be loaded from interface") =>
        // Related to: https://issues.scala-lang.org/browse/SI-10129
        LOG.warn(s"Unable to find referenced classes for: $outerClass. Related to Scala language issue: SI-10129", ae)
        None
      case t: Throwable => throw t
    }
  }

  private def getClassesForType(mirror: RuntimeMirror, typeSignature: Type): Seq[Class[_]] = typeSignature match {
    case TypeRef(_, _, args) =>
      args.flatMap { generic =>
        //If the wrapped type is a Tuple, recurse into its types
        if (generic.typeSymbol.fullName.startsWith("scala.Tuple")) {
          getClassesForType(mirror, generic)
        } else {
          getClassOpt(mirror, generic.typeSymbol)
        }
      }
    //.member returns the accessor method for the variable unless the field is private[this], so inspect the return type
    case NullaryMethodType(resultType) => getClassesForType(mirror, resultType)
    case _ => Nil
  }

  private def getClassOpt(mirror: RuntimeMirror, typeSymbol: Symbol): Option[Class[_]] = {
    try {
      Some(mirror.runtimeClass(typeSymbol.asClass))
    } catch {
      case _: ClassNotFoundException | ScalaReflectionException(_) => None
    }
  }
}
