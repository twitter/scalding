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
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.io.serializer.{ Serialization => HSerialization }
import com.twitter.chill.{ ExternalizerCodec, ExternalizerInjection, Externalizer, KryoInstantiator }
import com.twitter.chill.config.{ ScalaMapConfig, ConfiguredInstantiator }
import com.twitter.bijection.{ Base64String, Injection }
import com.twitter.scalding.filecache.{CachedFile, DistributedCacheFile, HadoopCachedFile}

import cascading.pipe.assembly.AggregateBy
import cascading.flow.{ FlowListener, FlowStepListener, FlowProps, FlowStepStrategy }
import cascading.property.AppProps
import cascading.tuple.collect.SpillableProps

import java.security.MessageDigest
import java.net.URI

import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }
import com.twitter.scalding.serialization.RequireOrderedSerializationMode

/**
 * This is a wrapper class on top of Map[String, String]
 */
abstract class Config extends Serializable {

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

  def getBoolean(key: String, orElse: => Boolean): Boolean =
    get(key).map(_.toBoolean).getOrElse(orElse)

  /**
   * Add files to be localized to the config. Intended to be used by user code.
   * @param cachedFiles CachedFiles to be added
   * @return new Config with cached files
   */
  def addDistributedCacheFiles(cachedFiles: CachedFile*): Config =
    cachedFiles.foldLeft(this) { case (config, file) =>
        file match {
          case hadoopFile: HadoopCachedFile =>
            Config.addDistributedCacheFile(hadoopFile.sourceUri, config)
          case _ => config
        }
    }

  /**
   * Get cached files from config
   */
  def getDistributedCachedFiles: Seq[CachedFile] = {
    Config.getDistributedCacheFile(this)
  }

  /**
   * This is a name that if present is passed to flow.setName,
   * which should appear in the job tracker.
   */
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
    getClassForKey(AppProps.APP_JAR_CLASS)

  def getClassForKey(k: String): Option[Try[Class[_]]] =
    get(k).map { str =>
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

  def getMapSideAggregationThreshold: Option[Int] =
    get(AggregateBy.AGGREGATE_BY_THRESHOLD).map(_.toInt)

  @deprecated("Use setRequireOrderedSerializationMode", "12/14/17")
  def setRequireOrderedSerialization(b: Boolean): Config =
    this + (ScaldingRequireOrderedSerialization -> (b.toString))

  @deprecated("Use getRequireOrderedSerializationMode", "12/14/17")
  def getRequireOrderedSerialization: Boolean =
    getRequireOrderedSerializationMode == Some(RequireOrderedSerializationMode.Fail)

  /**
   * Set this configuration option to require all grouping/cogrouping
   * to use OrderedSerialization
   */
  def setRequireOrderedSerializationMode(r: Option[RequireOrderedSerializationMode]): Config =
    r.map {
      v => this + (ScaldingRequireOrderedSerialization -> (v.toString))
    }.getOrElse(this)

  def getRequireOrderedSerializationMode: Option[RequireOrderedSerializationMode] =
    get(ScaldingRequireOrderedSerialization)
      .map(_.toLowerCase()).collect {
        case "true" => RequireOrderedSerializationMode.Fail // backwards compatibility
        case "fail" => RequireOrderedSerializationMode.Fail
        case "log" => RequireOrderedSerializationMode.Log
      }

  def getCascadingSerializationTokens: Map[Int, String] =
    get(Config.CascadingSerializationTokens)
      .map(CascadingTokenUpdater.parseTokens)
      .getOrElse(Map.empty[Int, String])

  /**
   * This function gets the set of classes that have been registered to Kryo.
   * They may or may not be used in this job, but Cascading might want to be made aware
   * that these classes exist
   */
  def getKryoRegisteredClasses: Set[Class[_]] = {
    // Get an instance of the Kryo serializer (which is populated with registrations)
    getKryo.map { kryo =>
      val cr = kryo.newKryo.getClassResolver

      @annotation.tailrec
      def kryoClasses(idx: Int, acc: Set[Class[_]]): Set[Class[_]] =
        Option(cr.getRegistration(idx)) match {
          case Some(reg) => kryoClasses(idx + 1, acc + reg.getType)
          case None => acc // The first null is the end of the line
        }

      kryoClasses(0, Set[Class[_]]())
    }.getOrElse(Set())
  }

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
        classOf[cascading.tuple.hadoop.TupleSerialization],
        classOf[serialization.WrappedSerialization[_]])
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
    val withKryo = Config(chillConf.toMap + hadoopKV)

    val kryoClasses = withKryo.getKryoRegisteredClasses
      .filterNot(_.isPrimitive) // Cascading handles primitives and arrays
      .filterNot(_.isArray)

    withKryo.addCascadingClassSerializationTokens(kryoClasses)
  }

  /*
   * If a ConfiguredInstantiator has been set up, this returns it
   */
  def getKryo: Option[KryoInstantiator] =
    if (toMap.contains(ConfiguredInstantiator.KEY)) Some((new ConfiguredInstantiator(ScalaMapConfig(toMap))).getDelegate)
    else None

  def getArgs: Args = get(Config.ScaldingJobArgsSerialized) match {
    case None => new Args(Map.empty)
    case Some(str) => argsSerializer
      .invert(str)
      .map(new Args(_))
      .getOrElse(throw new RuntimeException(
        s"""Could not deserialize Args from Config. Maybe "$ScaldingJobArgsSerialized" was modified without using Config.setArgs?"""))
  }

  def setArgs(args: Args): Config =
    this
      .+(Config.ScaldingJobArgs -> args.toString)
      .+(Config.ScaldingJobArgsSerialized -> argsSerializer(args.m))

  def setDefaultComparator(clazz: Class[_ <: java.util.Comparator[_]]): Config =
    this + (FlowProps.DEFAULT_ELEMENT_COMPARATOR -> clazz.getName)

  def getOptimizationPhases: Option[Try[typed.OptimizationPhases]] =
    getClassForKey(Config.OptimizationPhases).map { tryClass =>
      tryClass.flatMap { clazz =>
        Try(clazz.newInstance().asInstanceOf[typed.OptimizationPhases])
      }
    }

  def setOptimizationPhases(clazz: Class[_ <: typed.OptimizationPhases]): Config =
    setOptimizationPhasesFromName(clazz.getName)

  def setOptimizationPhasesFromName(className: String): Config =
    this + (Config.OptimizationPhases -> className)

  def getScaldingVersion: Option[String] = get(Config.ScaldingVersion)
  def setScaldingVersion: Config =
    (this.+(Config.ScaldingVersion -> scaldingVersion)).+(
      // This is setting a property for cascading/driven
      (AppProps.APP_FRAMEWORKS -> ("scalding:" + scaldingVersion)))

  def getUniqueIds: Set[UniqueID] =
    get(UniqueID.UNIQUE_JOB_ID)
      .map { str => str.split(",").toSet[String].map(UniqueID(_)) }
      .getOrElse(Set.empty)

  /**
   * The serialization of your data will be smaller if any classes passed between tasks in your job
   * are listed here. Without this, strings are used to write the types IN EACH RECORD, which
   * compression probably takes care of, but compression acts AFTER the data is serialized into
   * buffers and spilling has been triggered.
   */
  def addCascadingClassSerializationTokens(clazzes: Set[Class[_]]): Config =
    CascadingTokenUpdater.update(this, clazzes)

  /*
   * This is *required* if you are using counters. You must use
   * the same UniqueID as you used when defining your jobs.
   */
  def addUniqueId(u: UniqueID): Config =
    update(UniqueID.UNIQUE_JOB_ID) {
      case None => (Some(u.get), ())
      case Some(str) => (Some((StringUtility.fastSplit(str, ",").toSet + u.get).mkString(",")), ())
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
        (s, UniqueID(StringUtility.fastSplit(str, ",").head))
    }

  /**
   * Set an ID to be shared across this usage of run for Execution
   */
  def setScaldingExecutionId(id: String): Config =
    this.+(ScaldingExecutionId -> id)

  def getScaldingExecutionId: Option[String] =
    get(ScaldingExecutionId)

  /*
   * Add this class name and the md5 hash of it into the config
   */
  def setScaldingFlowClass(clazz: Class[_]): Config =
    this.+(ScaldingFlowClassName -> clazz.getName).+(ScaldingFlowClassSignature -> Config.md5Identifier(clazz))

  def setScaldingFlowCounterValue(value: Long): Config =
    this + (ScaldingFlowCounterValue -> value.toString)

  def getScaldingFlowCounterValue: Option[Long] =
    get(ScaldingFlowCounterValue).map(_.toLong)

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
      case None => (Some(clsName), ())
      case Some(lst) => (Some(s"$clsName,$lst"), ())
    }._2

  /** Set the entire list of reducer estimators (overriding the existing list) */
  def setReducerEstimators(clsList: String): Config =
    this + (Config.ReducerEstimators -> clsList)

  /**
   * configure flow listeneres for observability
   */
  def addFlowListener(flowListenerProvider: (Mode, Config) => FlowListener): Config = {
    val serializedListener = flowListenerSerializer(flowListenerProvider)
    update(Config.FlowListeners) {
      case None => (Some(serializedListener), ())
      case Some(lst) => (Some(s"$serializedListener,$lst"), ())
    }._2
  }

  def getFlowListeners: List[Try[(Mode, Config) => FlowListener]] =
    get(Config.FlowListeners)
      .toList
      .flatMap(s => StringUtility.fastSplit(s, ","))
      .map(flowListenerSerializer.invert(_))

  def addFlowStepListener(flowListenerProvider: (Mode, Config) => FlowStepListener): Config = {
    val serializedListener = flowStepListenerSerializer(flowListenerProvider)
    update(Config.FlowStepListeners) {
      case None => (Some(serializedListener), ())
      case Some(lst) => (Some(s"$serializedListener,$lst"), ())
    }._2
  }

  def getFlowStepListeners: List[Try[(Mode, Config) => FlowStepListener]] =
    get(Config.FlowStepListeners)
      .toList
      .flatMap(s => StringUtility.fastSplit(s, ","))
      .map(flowStepListenerSerializer.invert(_))

  def addFlowStepStrategy(flowStrategyProvider: (Mode, Config) => FlowStepStrategy[JobConf]): Config = {
    val serializedListener = flowStepStrategiesSerializer(flowStrategyProvider)
    update(Config.FlowStepStrategies) {
      case None => (Some(serializedListener), ())
      case Some(lst) => (Some(s"$serializedListener,$lst"), ())
    }._2
  }

  def clearFlowStepStrategies: Config =
    this.-(Config.FlowStepStrategies)

  def getFlowStepStrategies: List[Try[(Mode, Config) => FlowStepStrategy[JobConf]]] =
    get(Config.FlowStepStrategies)
      .toList
      .flatMap(s => StringUtility.fastSplit(s, ","))
      .map(flowStepStrategiesSerializer.invert(_))

  /** Get the number of reducers (this is the parameter Hadoop will use) */
  def getNumReducers: Option[Int] = get(Config.HadoopNumReducers).map(_.toInt)
  def setNumReducers(n: Int): Config = this + (Config.HadoopNumReducers -> n.toString)

  /** Set username from System.used for querying hRaven. */
  def setHRavenHistoryUserName: Config =
    this + (Config.HRavenHistoryUserName -> System.getProperty("user.name"))

  def setHashJoinAutoForceRight(b: Boolean): Config =
    this + (HashJoinAutoForceRight -> (b.toString))

  def getHashJoinAutoForceRight: Boolean =
    getBoolean(HashJoinAutoForceRight, false)

  /**
   * Set to true to enable very verbose logging during FileSource's validation and planning.
   * This can help record what files were present / missing at runtime. Should only be enabled
   * for debugging.
   */
  def setVerboseFileSourceLogging(b: Boolean): Config =
    this + (VerboseFileSourceLoggingKey -> b.toString)

  def getSkipNullCounters: Boolean =
    getBoolean(SkipNullCounters, false)

  /**
   * If this is true, on hadoop, when we get a null Counter
   * for a given name, we just ignore the counter instead
   * of NPE
   */
  def setSkipNullCounters(boolean: Boolean): Config =
    this + (SkipNullCounters -> boolean.toString)

  /**
   * When this value is true, all temporary output is removed
   * when the outer-most execution completes, not on JVM shutdown.
   *
   * When you do .forceToDiskExecution or .toIterableExecution
   * we need to materialize the data somewhere. We can't be sure
   * that when the outer most execution is complete that all reads
   * have been done, since they could escape the value of
   * the Execution. If you know no such reference escapes, it
   * is safe to set to true.
   *
   * Note, this is *always* safe for Execution[Unit], a common
   * value.
   */
  def setExecutionCleanupOnFinish(boolean: Boolean): Config =
    this + (ScaldingExecutionCleanupOnFinish -> boolean.toString)

  /**
   * should we cleanup temporary files when
   * the outer most Execution is run.
   *
   * Not safe if the outer-most execution returns
   * a TypedPipe or Iterable derived from a forceToDiskExecution
   * or a toIterableExecution
   */
  def getExecutionCleanupOnFinish: Boolean =
    getBoolean(ScaldingExecutionCleanupOnFinish, false)

  // we use Config as a key in Execution caches so we
  // want to avoid recomputing it repeatedly
  override lazy val hashCode = toMap.hashCode
  override def equals(that: Any) = that match {
    case thatConf: Config =>
      if (this eq thatConf) true
      else if (hashCode != thatConf.hashCode) false
      else toMap == thatConf.toMap
    case _ => false
  }
}

object Config {
  val CascadingAppName: String = "cascading.app.name"
  val CascadingAppId: String = "cascading.app.id"
  val CascadingSerializationTokens = "cascading.serialization.tokens"
  val IoSerializationsKey: String = "io.serializations"
  val ScaldingFlowClassName: String = "scalding.flow.class.name"
  val ScaldingFlowClassSignature: String = "scalding.flow.class.signature"
  /**
   * This is incremented every time a cascading flow is run as an Execution
   */
  val ScaldingFlowCounterValue: String = "scalding.flow.counter.value"
  val ScaldingFlowSubmittedTimestamp: String = "scalding.flow.submitted.timestamp"
  val ScaldingExecutionId: String = "scalding.execution.uuid"
  val ScaldingExecutionCleanupOnFinish: String = "scalding.execution.cleanup.onfinish"
  val ScaldingJobArgs: String = "scalding.job.args"
  val ScaldingJobArgsSerialized: String = "scalding.job.argsserialized"
  val ScaldingVersion: String = "scalding.version"
  val SkipNullCounters: String = "scalding.counters.skipnull"
  val HRavenHistoryUserName: String = "hraven.history.user.name"
  val ScaldingRequireOrderedSerialization: String = "scalding.require.orderedserialization"
  val FlowListeners: String = "scalding.observability.flowlisteners"
  val FlowStepListeners: String = "scalding.observability.flowsteplisteners"
  val FlowStepStrategies: String = "scalding.strategies.flowstepstrategies"
  val VerboseFileSourceLoggingKey: String = "scalding.filesource.verbose.logging"
  val OptimizationPhases: String = "scalding.optimization.phases"
  val RuntimeFrameworkKey = "mapreduce.framework.name"
  val RuntimeFrameworkValueLocal = "local"

  /**
   * Parameter that actually controls the number of reduce tasks.
   * Be sure to set this in the JobConf for the *step* not the flow.
   */
  val HadoopNumReducers = "mapred.reduce.tasks"

  /** Name of parameter to specify which class to use as the default estimator. */
  val ReducerEstimators = "scalding.reducer.estimator.classes"

  /** Whether estimator should override manually-specified reducers. */
  val ReducerEstimatorOverride = "scalding.reducer.estimator.override"

  /** Whether the number of reducers has been set explicitly using a `withReducers` */
  val WithReducersSetExplicitly = "scalding.with.reducers.set.explicitly"

  /** Name of parameter to specify which class to use as the default estimator. */
  val MemoryEstimators = "scalding.memory.estimator.classes"

  /** Hadoop map memory */
  val MapMemory = "mapreduce.map.memory.mb"

  /** Hadoop map java opts */
  val MapJavaOpts = "mapreduce.map.java.opts"

  /** Hadoop reduce java opts */
  val ReduceJavaOpts = "mapreduce.reduce.java.opts"

  /** Hadoop reduce memory */
  val ReduceMemory = "mapreduce.reduce.memory.mb"

  /** Manual description for use in .dot and MR step names set using a `withDescription`. */
  val PipeDescriptions = "scalding.pipe.descriptions"
  val StepDescriptions = "scalding.step.descriptions"

  /**
   * Parameter that can be used to determine behavior on the rhs of a hashJoin.
   * If true, we try to guess when to auto force to disk before a hashJoin
   * else (the default) we don't try to infer this and the behavior can be dictated by the user manually
   * calling forceToDisk on the rhs or not as they wish.
   */
  val HashJoinAutoForceRight: String = "scalding.hashjoin.autoforceright"

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

  /*
   * Extensions to the Default Config to tune it for unit tests
   */
  def unitTestDefault: Config =
    Config(Config.default.toMap ++ Map("cascading.update.skip" -> "true", RuntimeFrameworkKey -> RuntimeFrameworkValueLocal))

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
    val asMap = conf.toMap.toMap[K, V] // linter:disable:TypeToType // we are upcasting K, V
    val duplicateKeys = (m.keySet & asMap.keySet)
    if (duplicateKeys.isEmpty) Right(m ++ asMap)
    else Left(conf.toMap.keySet.filter(duplicateKeys(_))) // make sure to return Set[String], and not cast
  }
  /**
   * This overwrites any keys in m that exist in config.
   */
  def overwrite[K >: String, V >: String](m: Map[K, V], conf: Config): Map[K, V] =
    m ++ (conf.toMap.toMap[K, V]) // linter:disable:TypeToType // we are upcasting K, V

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

  /**
   * Add a file to be localized to the config. Intended to be used by user code.
   *
   * @param qualifiedURI The qualified uri of the cache to be localized
   * @param config Config to add the cache to
   *
   * @return new Config with cached files
   *
   * @see basic logic from [[org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile]]
   */
  private def addDistributedCacheFile(qualifiedURI: URI, config: Config): Config = {
    val newFile = DistributedCacheFile
      .symlinkedUriFor(qualifiedURI)
      .toString

    val newFiles = config
      .get(MRJobConfig.CACHE_FILES)
      .map(files => files + "," + newFile)
      .getOrElse(newFile)

    config + (MRJobConfig.CACHE_FILES -> newFiles)
  }

  /**
   * Get distributed cache files from config
   *
   * @param config Config with cached files
   */
  private def getDistributedCacheFile(config: Config): Seq[CachedFile] = {
    config
      .get(MRJobConfig.CACHE_FILES)
      .toSeq
      .flatMap(_.split(","))
      .filter(_.nonEmpty)
      .map { file =>
        val symlinkedUri = new URI(file)
        val qualifiedUri = new URI(symlinkedUri.getScheme, symlinkedUri.getSchemeSpecificPart, null)
        HadoopCachedFile(qualifiedUri)
      }
  }

  private[this] def buildInj[T: ExternalizerInjection: ExternalizerCodec]: Injection[T, String] =
    Injection.connect[T, Externalizer[T], Array[Byte], Base64String, String]

  @transient private[scalding] lazy val flowStepListenerSerializer = buildInj[(Mode, Config) => FlowStepListener]
  @transient private[scalding] lazy val flowListenerSerializer = buildInj[(Mode, Config) => FlowListener]
  @transient private[scalding] lazy val flowStepStrategiesSerializer = buildInj[(Mode, Config) => FlowStepStrategy[JobConf]]
  @transient private[scalding] lazy val argsSerializer = buildInj[Map[String, List[String]]]
}
