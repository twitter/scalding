package com.twitter.scalding.spark_backend

import com.twitter.scalding.{Config, Mode, WritableSequenceFile, TextLine}
import com.twitter.scalding.typed.{ Resolver, TypedSource, TypedSink }
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import scala.concurrent.{ Future, ExecutionContext }
import scala.reflect.ClassTag
import org.apache.hadoop.io.Writable

case class SparkMode(session: SparkSession, sources: Resolver[TypedSource, SparkSource], sink: Resolver[TypedSink, SparkSink]) extends Mode {
  def newWriter(): SparkWriter =
    new SparkWriter(this)
}

object SparkMode {
  /**
   * A Sparkmode with no sources or sink support.
   * Only useful for testing, or working flows that never
   * read or write from disk
   */
  def empty(session: SparkSession): SparkMode =
    SparkMode(session, Resolver.empty, Resolver.empty)

  /**
   * set up the default sources and sinks, which support
   * some of scalding's built in sources and sinks
   */
  def default(session: SparkSession): SparkMode =
    SparkMode(session, SparkSource.Default, SparkSink.Default)

  implicit class SparkConfigMethods(val conf: Config) extends AnyVal {
    def getForkPersistMode: Option[StorageLevel] =
      conf.get("scalding.spark.fork.persist").map(StorageLevel.fromString(_))
    def setForkPersistMode(str: String): Config = {
      // make sure this does not throw:
      StorageLevel.fromString(str)
      conf + ("scalding.spark.fork.persist" -> str)
    }
    def getForceToDiskPersistMode: Option[StorageLevel] =
      conf.get("scalding.spark.forcetodisk.persist").map(StorageLevel.fromString(_))
    def setForceToDiskPersistMode(str: String): Config = {
      // make sure this does not throw:
      StorageLevel.fromString(str)
      conf + ("scalding.spark.forcetodisk.persist" -> str)
    }
    def getMaxPartitionCount: Option[Int] =
      conf.get("scalding.spark.maxpartitioncount").map(_.toInt)
    def setMaxPartitionCount(c: Int): Config = {
      require(c > 0, s"expected maxpartitioncount to be > 0, got $c")
      conf + ("scalding.spark.maxpartitioncount" -> c.toString)
    }
    def getReducerScaling: Option[Double] =
      conf.get("scalding.spark.reducerscaling").map(_.toDouble)
    def setReducerScaling(c: Double): Config = {
      require(c > 0, s"expected reducerscaling to be > 0, got $c")
      conf + ("scalding.spark.reducerscaling" -> c.toString)
    }
  }
}

trait SparkSource[+A] extends Serializable {
  def read(session: SparkSession, config: Config)(implicit ec: ExecutionContext): Future[RDD[_ <: A]]
}

object SparkSource extends Serializable {
  def textLine(path: String, parts: Option[Int]): SparkSource[String] =
    new SparkSource[String] {
      override def read(session: SparkSession, config: Config)(implicit ec: ExecutionContext): Future[RDD[_ <: String]] = {
        val partitions = parts.orElse(config.getNumReducers).getOrElse(10)
        Future(session.sparkContext.textFile(path, partitions))
      }
    }

  def writableSequenceFile[K <: Writable, V <: Writable](path: String,
    kclass: Class[K],
    vclass: Class[V]): SparkSource[(K, V)] = new SparkSource[(K, V)] {
    override def read(session: SparkSession, config: Config)(implicit ec: ExecutionContext): Future[RDD[_ <: (K, V)]] = {
      Future(session.sparkContext.sequenceFile[K, V](path, kclass, vclass))
    }
  }

  /**
   * This has a mappings for some built in scalding sources
   * currently only WritableSequenceFile and TextLine are supported
   *
   * users can add their own implementations and compose Resolvers using orElse
   */
  val Default: Resolver[TypedSource, SparkSource] =
    new Resolver[TypedSource, SparkSource] {
      def apply[A](i: TypedSource[A]): Option[SparkSource[A]] = {
        i match {
          case ws @ WritableSequenceFile(path, _, _) =>
              Some(writableSequenceFile(path, ws.keyType, ws.valueType))
          case tl: TextLine =>
            // actually only one path:
            Some(textLine(tl.localPaths.head, None))
          case _ =>
            None
        }
      }
    }
}

trait SparkSink[-A] extends Serializable {
  def write(session: SparkSession, config: Config, rdd: RDD[_ <: A])(implicit ec: ExecutionContext): Future[Unit]
}

object SparkSink extends Serializable {
  def writableSequenceFile[K <: Writable, V <: Writable](
    path: String,
    keyClass: Class[K],
    valClass: Class[V]): SparkSink[(K, V)] = new SparkSink[(K, V)] {
    override def write(session: SparkSession, config: Config, rdd: RDD[_ <: (K, V)])(implicit ec: ExecutionContext): Future[Unit] = {
      // first widen to (K, V)
      implicit val ck: ClassTag[K] = ClassTag[K](keyClass)
      implicit val cv: ClassTag[V] = ClassTag[V](valClass)
      Future(rdd.asInstanceOf[RDD[(K, V)]].saveAsSequenceFile(path, None))
    }
  }

  def textLine(path: String): SparkSink[String] =
    new SparkSink[String] {
      override def write(session: SparkSession, config: Config, rdd: RDD[_ <: String])(implicit ec: ExecutionContext): Future[Unit] = {
        Future(rdd.saveAsTextFile(path))
      }
    }
  /**
   * This has a mappings for some built in scalding sinks
   * currently only WritableSequenceFile and TextLine are supported
   *
   * users can add their own implementations and compose Resolvers using orElse
   */
  val Default: Resolver[TypedSink, SparkSink] =
    new Resolver[TypedSink, SparkSink] {
      def apply[A](i: TypedSink[A]): Option[SparkSink[A]] = {
        i match {
          case ws @ WritableSequenceFile(path, fields, sinkMode) =>
            Some(writableSequenceFile(path, ws.keyType, ws.valueType).asInstanceOf[SparkSink[A]])
          case tl: TextLine =>
            Some(textLine(tl.localPaths.head).asInstanceOf[SparkSink[A]])
          case _ =>
            None
        }
      }
    }
}

