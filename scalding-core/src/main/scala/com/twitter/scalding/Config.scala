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
package com.twitter.scalding

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.serializer.{ Serialization => HSerialization }
import com.twitter.chill.KryoInstantiator
import com.twitter.chill.config.{ ScalaMapConfig, ScalaAnyRefMapConfig, ConfiguredInstantiator }
import com.twitter.scalding.reducer_estimation.ReducerEstimator

import cascading.pipe.assembly.AggregateBy
import cascading.flow.{ FlowStepStrategy, FlowProps }
import cascading.property.AppProps
import cascading.tuple.collect.SpillableProps

import java.security.MessageDigest
import java.util.UUID

import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

/**
 * This is a wrapper class on top of Map[String, String]
 */
trait Config {

  import Config._ // get the constants
  def toMap: Map[String, String]

  def get(key: String): Option[String] = toMap.get(key)
  def +(kv: (String, String)): Config = Config(toMap + kv)
  def ++(that: Config): Config = Config(toMap ++ that.toMap)
  def -(k: String): Config = Config(toMap - k)
  def update[R](k: String)(fn: Option[String] => (Option[String], R)): (R, Config) =
    fn(get(k)) match {
      case (Some(v), r) => (r, this + (k -> v))
      case (None, r) => (r, this - k)
    }

  def getCascadingAppName: Option[String] = get(CascadingAppName)
  def setCascadingAppName(name: String): Config =
    this + (CascadingAppName -> name)

  def setCascadingAppId(id: String): Config =
    this + (CascadingAppId -> id)

  /**
   * Non-fat-jar use cases require this, BUT using it
   * with fat jars can cause problems. It is not
   * set by default, but if you have problems you
   * might need to set the Job class here
   * Consider also setting this same class here:
   * setScaldingFlowClass
   */
  def setCascadingAppJar(clazz: Class[_]): Config =
    this + (AppProps.APP_JAR_CLASS -> clazz.getName)

  /**
   * Returns None if not set, otherwise reflection
   * is used to create the Class.forName
   */
  def getCascadingAppJar: Option[Try[Class[_]]] =
    get(AppProps.APP_JAR_CLASS).map { str =>
      // The Class[_] messes up using Try(Class.forName(str)) on scala 2.9.3
      try {
        Success(
          // Make sure we are using the class-loader for the current thread
          Class.forName(str, true, Thread.currentThread().getContextClassLoader))
      } catch { case err: Throwable => Failure(err) }
    }

  /*
   * Used in joins to determine how much of the "right hand side" of
   * the join to keep in memory
   */
  def setListSpillThreshold(count: Int): Config =
    this + (SpillableProps.LIST_THRESHOLD -> count.toString)

  /*
   * Used in hashJoin/joinWithTiny to determine how big the map
   * can be before spilling to disk. Generally, as big as you can
   * allow here without OOM will help performance.
   */
  def setMapSpillThreshold(count: Int): Config =
    this + (SpillableProps.MAP_THRESHOLD -> count.toString)

  /*
   * Used in map-side aggregation of associative operations (Semigroup/Monoid)
   * This controls how many keys are in an in-memory cache. If a significant
   * probability mass of the key-space is far bigger than this value, it
   * does not help much (and may hurt, so experiment with disabling to get
   * the best results
   */
  def setMapSideAggregationThreshold(count: Int): Config =
    this + (AggregateBy.AGGREGATE_BY_THRESHOLD -> count.toString)

  /*
   * Hadoop and Cascading serialization needs to be first, and the Kryo serialization
   * needs to be last and this method handles this for you:
   * hadoop, cascading, [userHadoop,] kyro
   * is the order.
   *
   * Kryo uses the ConfiguredInstantiator, which is configured either by reflection:
   * Right(classOf[MyInstantiator]) or by serializing given Instantiator instance
   * with a class to serialize to bootstrap the process:
   * Left((classOf[serialization.KryoHadoop], myInstance))
   */
  def setSerialization(kryo: Either[(Class[_ <: KryoInstantiator], KryoInstantiator), Class[_ <: KryoInstantiator]],
    userHadoop: Seq[Class[_ <: HSerialization[_]]] = Nil): Config = {

    // Hadoop and Cascading should come first
    val first: Seq[Class[_ <: HSerialization[_]]] =
      Seq(classOf[org.apache.hadoop.io.serializer.WritableSerialization],
        classOf[cascading.tuple.hadoop.TupleSerialization])
    // this must come last
    val last: Seq[Class[_ <: HSerialization[_]]] = Seq(classOf[com.twitter.chill.hadoop.KryoSerialization])
    val required = (first ++ last).toSet[AnyRef] // Class is invariant, but we use it as a function
    // Make sure we keep the order correct and don't add the required fields twice
    val hadoopSer = first ++ (userHadoop.filterNot(required)) ++ last

    val hadoopKV = (Config.IoSerializationsKey -> hadoopSer.map(_.getName).mkString(","))

    // Now handle the Kryo portion which uses another mechanism
    val chillConf = ScalaMapConfig(toMap)
    kryo match {
      case Left((bootstrap, inst)) => ConfiguredInstantiator.setSerialized(chillConf, bootstrap, inst)
      case Right(refl) => ConfiguredInstantiator.setReflect(chillConf, refl)
    }
    Config(chillConf.toMap + hadoopKV)
  }

  /*
   * If a ConfiguredInstantiator has been set up, this returns it
   */
  def getKryo: Option[KryoInstantiator] =
    if (toMap.contains(ConfiguredInstantiator.KEY)) Some((new ConfiguredInstantiator(ScalaMapConfig(toMap))).getDelegate)
    else None

  def getArgs: Args = get(Config.ScaldingJobArgs) match {
    case None => new Args(Map.empty)
    case Some(str) => Args(str)
  }

  def setArgs(args: Args): Config =
    this + (Config.ScaldingJobArgs -> args.toString)

  def setDefaultComparator(clazz: Class[_ <: java.util.Comparator[_]]): Config =
    this + (FlowProps.DEFAULT_ELEMENT_COMPARATOR -> clazz.getName)

  def getScaldingVersion: Option[String] = get(Config.ScaldingVersion)
  def setScaldingVersion: Config =
    (this.+(Config.ScaldingVersion -> scaldingVersion)).+(
      // This is setting a property for cascading/driven
      (AppProps.APP_FRAMEWORKS -> ("scalding:" + scaldingVersion.toString)))

  def getUniqueId: Option[UniqueID] =
    get(UniqueID.UNIQUE_JOB_ID).map(UniqueID(_))

  /*
   * This is *required* if you are using counters. You must use
   * the same UniqueID as you used when defining your jobs.
   */
  def addUniqueId(u: UniqueID): Config =
    update(UniqueID.UNIQUE_JOB_ID) {
      case None => (Some(u.get), ())
      case Some(str) => (Some((str.split(",").toSet + u.get).mkString(",")), ())
    }._2

  /**
   * Allocate a new UniqueID if there is not one present
   */
  def ensureUniqueId: (UniqueID, Config) =
    update(UniqueID.UNIQUE_JOB_ID) {
      case None =>
        val uid = UniqueID.getRandom
        (Some(uid.get), uid)
      case s @ Some(str) =>
        (s, UniqueID(str.split(",").head))
    }

  /*
   * Add this class name and the md5 hash of it into the config
   */
  def setScaldingFlowClass(clazz: Class[_]): Config =
    this.+(ScaldingFlowClassName -> clazz.getName).+(ScaldingFlowClassSignature -> Config.md5Identifier(clazz))

  def getSubmittedTimestamp: Option[RichDate] =
    get(ScaldingFlowSubmittedTimestamp).map { ts => RichDate(ts.toLong) }
  /*
   * Sets the timestamp only if it was not already set. This is here
   * to prevent overwriting the submission time if it was set by an
   * previously (or externally)
   */
  def maybeSetSubmittedTimestamp(date: RichDate = RichDate.now): (Option[RichDate], Config) =
    update(ScaldingFlowSubmittedTimestamp) {
      case s @ Some(ts) => (s, Some(RichDate(ts.toLong)))
      case None => (Some(date.timestamp.toString), None)
    }

  /**
   * Prepend an estimator so it will be tried first. If it returns None,
   * the previously-set estimators will be tried in order.
   */
  def addReducerEstimator[T](cls: Class[T]): Config =
    addReducerEstimator(cls.getName)

  /**
   * Prepend an estimator so it will be tried first. If it returns None,
   * the previously-set estimators will be tried in order.
   */
  def addReducerEstimator(clsName: String): Config =
    update(Config.ReducerEstimators) {
      case None => Some(clsName) -> ()
      case Some(lst) => Some(clsName + "," + lst) -> ()
    }._2

  /** Set the entire list of reducer estimators (overriding the existing list) */
  def setReducerEstimators(clsList: String): Config =
    this + (Config.ReducerEstimators -> clsList)

  /** Get the number of reducers (this is the parameter Hadoop will use) */
  def getNumReducers: Option[Int] = get(Config.HadoopNumReducers).map(_.toInt)
  def setNumReducers(n: Int): Config = this + (Config.HadoopNumReducers -> n.toString)

  /** Set username from System.used for querying hRaven. */
  def setHRavenHistoryUserName: Config =
    this + (Config.HRavenHistoryUserName -> System.getProperty("user.name"))

  override def hashCode = toMap.hashCode
  override def equals(that: Any) = that match {
    case thatConf: Config => toMap == thatConf.toMap
    case _ => false
  }
}

object Config {
  val CascadingAppName: String = "cascading.app.name"
  val CascadingAppId: String = "cascading.app.id"
  val IoSerializationsKey: String = "io.serializations"
  val ScaldingFlowClassName: String = "scalding.flow.class.name"
  val ScaldingFlowClassSignature: String = "scalding.flow.class.signature"
  val ScaldingFlowSubmittedTimestamp: String = "scalding.flow.submitted.timestamp"
  val ScaldingJobArgs: String = "scalding.job.args"
  val ScaldingVersion: String = "scalding.version"
  val HRavenHistoryUserName: String = "hraven.history.user.name"

  /**
   * Parameter that actually controls the number of reduce tasks.
   * Be sure to set this in the JobConf for the *step* not the flow.
   */
  val HadoopNumReducers = "mapred.reduce.tasks"

  /** Name of parameter to specify which class to use as the default estimator. */
  val ReducerEstimators = "scalding.reducer.estimator.classes"

  /** Whether estimator should override manually-specified reducers. */
  val ReducerEstimatorOverride = "scalding.reducer.estimator.override"

  val empty: Config = Config(Map.empty)

  /*
   * Here is a config that will work, but perhaps is not optimally tuned for
   * your cluster
   */
  def default: Config =
    empty
      .setListSpillThreshold(100 * 1000)
      .setMapSpillThreshold(100 * 1000)
      .setMapSideAggregationThreshold(100 * 1000)
      .setSerialization(Right(classOf[serialization.KryoHadoop]))
      .setScaldingVersion
      .setHRavenHistoryUserName

  /**
   * Merge Config.default with Hadoop config from the mode (if in Hadoop mode)
   */
  def defaultFrom(mode: Mode): Config =
    default ++ (mode match {
      case m: HadoopMode => Config.fromHadoop(m.jobConf) - IoSerializationsKey
      case _ => empty
    })

  def apply(m: Map[String, String]): Config = new Config { def toMap = m }
  /*
   * Implicits cannot collide in name, so making apply impliict is a bad idea
   */
  implicit def from(m: Map[String, String]): Config = apply(m)

  /*
   * Legacy code that uses Map[AnyRef, AnyRef] can call this
   * function to get a Config.
   * If there are unrecognized non-string values, this may fail.
   */
  def tryFrom(maybeConf: Map[AnyRef, AnyRef]): Try[Config] = {
    val (nonStrings, strings) = stringsFrom(maybeConf)
    val initConf = from(strings)

    (nonStrings
      .get(AppProps.APP_JAR_CLASS) match {
        case Some(clazz) =>
          // Again, the _ causes problem with Try
          try {
            val cls = classOf[Class[_]].cast(clazz)
            Success((nonStrings - AppProps.APP_JAR_CLASS, initConf.setCascadingAppJar(cls)))
          } catch {
            case err: Throwable => Failure(err)
          }
        case None => Success((nonStrings, initConf))
      })
      .flatMap {
        case (unhandled, withJar) =>
          if (unhandled.isEmpty) Success(withJar)
          else Failure(new Exception("unhandled configurations: " + unhandled.toString))
      }
  }

  /**
   * Returns all the non-string keys on the left, the string keys/values on the right
   */
  def stringsFrom[K >: String, V >: String](m: Map[K, V]): (Map[K, V], Map[String, String]) =
    m.foldLeft((Map.empty[K, V], Map.empty[String, String])) {
      case ((kvs, conf), kv) =>
        kv match {
          case (ks: String, vs: String) => (kvs, conf + (ks -> vs))
          case _ => (kvs + kv, conf)
        }
    }

  /**
   * Either union these two, or return the keys that overlap
   */
  def disjointUnion[K >: String, V >: String](m: Map[K, V], conf: Config): Either[Set[String], Map[K, V]] = {
    val asMap = conf.toMap.toMap[K, V]
    val duplicateKeys = (m.keySet & asMap.keySet)
    if (duplicateKeys.isEmpty) Right(m ++ asMap)
    else Left(conf.toMap.keySet.filter(duplicateKeys(_))) // make sure to return Set[String], and not cast
  }
  /**
   * This overwrites any keys in m that exist in config.
   */
  def overwrite[K >: String, V >: String](m: Map[K, V], conf: Config): Map[K, V] =
    m ++ (conf.toMap.toMap[K, V])

  /*
   * Note that Hadoop Configuration is mutable, but Config is not. So a COPY is
   * made on calling here. If you need to update Config, you do it by modifying it.
   * This copy also forces all expressions in values to be evaluated, freezing them
   * as well.
   */
  def fromHadoop(conf: Configuration): Config =
    // use `conf.get` to force JobConf to evaluate expressions
    Config(conf.asScala.map { e => e.getKey -> conf.get(e.getKey) }.toMap)

  /*
   * For everything BUT SERIALIZATION, this prefers values in conf,
   * but serialization is generally required to be set up with Kryo
   * (or some other system that handles general instances at runtime).
   */
  def hadoopWithDefaults(conf: Configuration): Config =
    (empty
      .setListSpillThreshold(100 * 1000)
      .setMapSpillThreshold(100 * 1000)
      .setMapSideAggregationThreshold(100 * 1000) ++ fromHadoop(conf))
      .setSerialization(Right(classOf[serialization.KryoHadoop]))
      .setScaldingVersion
  /*
   * This can help with versioning Class files into configurations if they are
   * logged. This allows you to detect changes in the job logic that may correlate
   * with changes in performance
   */
  def md5Identifier(clazz: Class[_]): String = {
    def fromInputStream(s: java.io.InputStream): Array[Byte] =
      Stream.continually(s.read).takeWhile(-1 !=).map(_.toByte).toArray

    def toHexString(bytes: Array[Byte]): String = bytes.map("%02X".format(_)).mkString

    def md5Hex(bytes: Array[Byte]): String = {
      val md = MessageDigest.getInstance("MD5")
      md.update(bytes)
      toHexString(md.digest)
    }

    val classAsPath = clazz.getName.replace(".", "/") + ".class"
    val is = clazz.getClassLoader.getResourceAsStream(classAsPath)
    val bytes = fromInputStream(is)
    is.close()
    md5Hex(bytes)
  }
}
