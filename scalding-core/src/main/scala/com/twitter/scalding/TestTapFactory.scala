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

import com.twitter.maple.tap.MemorySourceTap
import cascading.scheme.Scheme
import cascading.tuple.Fields
import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tap.hadoop.Hfs
import cascading.scheme.NullScheme

import java.io.{ Serializable, InputStream, OutputStream }

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import scala.collection.JavaConverters._

/**
 * Use this to create Taps for testing.
 */
object TestTapFactory extends Serializable {
  val sourceNotFoundError: String = "Source %s does not appear in your test sources.  Make sure " +
    "each source in your job has a corresponding source in the test sources that is EXACTLY " +
    "equal.  Call the '.source' or '.sink' methods as appropriate on your JobTest to add test " +
    "buffers for each source or sink."

  def apply(src: Source, fields: Fields, sinkMode: SinkMode = SinkMode.REPLACE): TestTapFactory = new TestTapFactory(src, sinkMode) {
    override def sourceFields: Fields = fields
    override def sinkFields: Fields = fields
  }
  def apply[A, B](src: Source, scheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], A, B]): TestTapFactory = apply(src, scheme, SinkMode.REPLACE)
  def apply[A, B](src: Source,
    scheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], A, B], sinkMode: SinkMode): TestTapFactory =
    new TestTapFactory(src, sinkMode) { override def hdfsScheme = Some(scheme) }
}

class TestTapFactory(src: Source, sinkMode: SinkMode) extends Serializable {
  def sourceFields: Fields =
    hdfsScheme.map { _.getSourceFields }.getOrElse(sys.error("No sourceFields defined"))

  def sinkFields: Fields =
    hdfsScheme.map { _.getSinkFields }.getOrElse(sys.error("No sinkFields defined"))

  def hdfsScheme: Option[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]] = None

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    mode match {
      case Test(buffers) => {
        /*
        * There MUST have already been a registered sink or source in the Test mode.
        * to access this.  You must explicitly name each of your test sources in your
        * JobTest.
        */
        require(
          buffers(src).isDefined,
          TestTapFactory.sourceNotFoundError.format(src))
        val buffer =
          if (readOrWrite == Write) {
            val buf = buffers(src).get
            //Make sure we wipe it out:
            buf.clear()
            buf
          } else {
            // if the source is also used as a sink, we don't want its contents to get modified
            buffers(src).get.clone()
          }
        new MemoryTap[InputStream, OutputStream](
          new NullScheme(sourceFields, sinkFields),
          buffer)
      }
      case hdfsTest @ HadoopTest(conf, buffers) =>
        readOrWrite match {
          case Read => {
            val bufOpt = buffers(src)
            if (bufOpt.isDefined) {
              val buffer = bufOpt.get
              val fields = sourceFields
              (new MemorySourceTap(buffer.toList.asJava, fields)).asInstanceOf[Tap[JobConf, _, _]]
            } else {
              CastHfsTap(new Hfs(hdfsScheme.get, hdfsTest.getWritePathFor(src), sinkMode))
            }
          }
          case Write => {
            val path = hdfsTest.getWritePathFor(src)
            CastHfsTap(new Hfs(hdfsScheme.get, path, sinkMode))
          }
        }
      case _ => {
        throw new RuntimeException("TestTapFactory doesn't support mode: " + mode.toString)
      }
    }
  }
}
