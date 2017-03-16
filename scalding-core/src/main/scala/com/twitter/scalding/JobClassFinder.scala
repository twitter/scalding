package com.twitter.scalding

import com.twitter.scalding.typed.CoGroupable

import scala.reflect.runtime.universe

object JobClassFinder {
  private val baseContainers = List(
    classOf[Execution[_]],
    classOf[TypedPipe[_]],
    classOf[TypedSink[_]],
    classOf[CoGroupable[_, _]],
    classOf[KeyedList[_, _]]
  )
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
  def findUsedClasses(jobClazz: Class[_ <: Job]): Set[Class[_]] = {
    val mirror = universe.runtimeMirror(jobClazz.getClassLoader)
    val scalaType =  mirror.classSymbol(jobClazz).toType
    (for {
      field <- jobClazz.getDeclaredFields
      if baseContainers.exists(_.isAssignableFrom(field.getType))
      scalaSignature = scalaType.member(universe.stringToTermName(field.getName)).typeSignature
      clazz <- getClassesForType(scalaSignature)
    } yield {
      clazz
    }).toSet
  }

  private def getClassesForType(typeSignature: universe.Type): Seq[Class[_]] = typeSignature match {
    case universe.NullaryMethodType(resultType) => resultType match {
      case universe.TypeRef(_, _, args) =>
        args.flatMap { generic =>
          //If the wrapped type is a Tuple2, recurse into its types
          if (generic.typeSymbol.fullName == "scala.Tuple2") {
            getClassesForType(generic)
          } else {
            getClassOpt(generic.typeSymbol.fullName)
          }
        }
      case _ => Nil
    }
    case _ => Nil
  }

  private def getClassOpt(name: String): Option[Class[_]] = {
    try {
      Some(Class.forName(name))
    } catch {
      case _: ClassNotFoundException => None
    }
  }
}
