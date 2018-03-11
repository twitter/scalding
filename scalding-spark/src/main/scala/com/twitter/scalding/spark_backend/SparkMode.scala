package com.twitter.scalding.spark_backend

import com.twitter.scalding.{ Config, Mode }
import com.twitter.scalding.typed.{ Resolver, TypedSource, TypedSink }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.concurrent.{ Future, ExecutionContext, Promise }

case class SparkMode(sources: Resolver[TypedSource, SparkSource], sink: Resolver[TypedSink, SparkSink]) extends Mode {
  def newWriter(): SparkWriter =
    new SparkWriter(this)
}

trait SparkSource[A] {
  def read(ctx: SparkContext, config: Config)(implicit ec: ExecutionContext): Future[RDD[A]]
}

trait SparkSink[A] {
  def write(ctx: SparkContext, config: Config, rdd: RDD[A])(implicit ec: ExecutionContext): Future[Unit]
}
