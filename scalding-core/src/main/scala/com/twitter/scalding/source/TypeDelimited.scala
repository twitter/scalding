package com.twitter.scalding.source

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import com.twitter.scalding._

/**
 * Trait to assist with creating objects such as [[TypedTsv]] to read from separated files.
 * Override separator, skipHeader, writeHeader as needed.
 */
trait TypedSeperatedFile extends Serializable {
  def separator: String
  def skipHeader: Boolean = false
  def writeHeader: Boolean = false

  def apply[T: Manifest: TupleConverter](path: String): FixedPathTypedDelimited[T] =
    apply(Seq(path))

  def apply[T: Manifest: TupleConverter](paths: Seq[String]): FixedPathTypedDelimited[T] =
    new FixedPathTypedDelimited[T](paths, skipHeader, writeHeader, separator)
}

/**
 * Typed tab separated values file
 */
object Tsv extends TypedSeperatedFile {
  val separator = "\t"
}

/**
 * Allows you to set the types, prefer this:
 * If T is a subclass of Product, we assume it is a tuple. If it is not, wrap T in a Tuple1:
 * e.g. TypedTsv[Tuple1[List[Int]]]
 */
trait TypedDelimited[T] extends FixedPathSource[T, RDD[T]] with BaseRDD[T] {
  val skipHeader: Boolean = false
  val writeHeader: Boolean = false
  val separator: String = "\t"

  implicit val mf: Manifest[T]
  implicit val conv: TupleConverter[T]

  def converter[U >: T] = TupleConverter.asSuperConverter[T, U](conv)

  def toRDD(implicit sparkContext: SparkContext, mode: Mode): RDD[T] = {
    val rddSet: Iterable[RDD[String]] = if (skipHeader == true) {
      goodPaths(Job.getInstance).map { p =>
        sparkContext.wholeTextFiles(p).map(_._2).flatMap { contents =>
          contents.split('\n').tail
        }
      }
    } else {
      goodPaths(Job.getInstance).map { p =>
        sparkContext.textFile(p)
      }
    }
    val data: RDD[String] = rddSet.reduce(_ ++ _)
    data.filter(_.size > 0).map { l =>
      val tups = l.split(separator).toIndexedSeq
      require(tups.size == conv.arity, s"Invalid line in tsv, not matching supplied type arity. Got : ${tups.size}, wanted: ${conv.arity}\n got: ${l}")
      conv(tups)
    }
  }
}

class FixedPathTypedDelimited[T](paths: Seq[String],
  override val skipHeader: Boolean = false,
  override val writeHeader: Boolean = false,
  override val separator: String = "\t")(implicit override val mf: Manifest[T], override val conv: TupleConverter[T])
  extends FixedPathSource[T, RDD[T]](paths: _*)
  with TypedDelimited[T] {

  override lazy val toString: String = "FixedPathTypedDelimited" +
    ((paths, skipHeader, writeHeader, separator, mf).toString)

  override def equals(that: Any): Boolean = Option(that)
    .map { _.toString == this.toString }.getOrElse(false)

  override lazy val hashCode: Int = toString.hashCode
}
