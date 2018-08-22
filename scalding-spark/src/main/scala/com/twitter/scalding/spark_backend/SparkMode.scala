package com.twitter.scalding.spark_backend

import com.twitter.scalding.{ Config, Mode }
import com.twitter.scalding.typed.{ Resolver, TypedSource, TypedSink }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.concurrent.{ Future, ExecutionContext }

case class SparkMode(session: SparkSession, sources: Resolver[TypedSource, SparkSource], sink: Resolver[TypedSink, SparkSink]) extends Mode {
  def newWriter(): SparkWriter =
    new SparkWriter(this)
}

object SparkMode {
  def empty(session: SparkSession): SparkMode =
    SparkMode(session, Resolver.empty, Resolver.empty)
}

trait SparkSource[+A] {
  def read(session: SparkSession, config: Config)(implicit ec: ExecutionContext): Future[RDD[_ <: A]]
}

trait SparkSink[-A] {
  def write(session: SparkSession, config: Config, rdd: RDD[_ <: A])(implicit ec: ExecutionContext): Future[Unit]
}
