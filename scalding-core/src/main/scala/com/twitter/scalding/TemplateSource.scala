/*
Copyright 2013 Inkling, Inc.

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

import cascading.tap.hadoop.Hfs
import cascading.tap.hadoop.{ TemplateTap => HTemplateTap }
import cascading.tap.local.FileTap
import cascading.tap.local.{ TemplateTap => LTemplateTap }
import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tuple.Fields

/**
 * This is a base class for template based output sources
 */
abstract class TemplateSource extends SchemedSource with HfsTapProvider {

  // The root path of the templated output.
  def basePath: String
  // The template as a java Formatter string. e.g. %s/%s for a two part template.
  def template: String
  // The fields to apply to the template.
  def pathFields: Fields = Fields.ALL

  /**
   * Creates the template tap.
   *
   * @param readOrWrite Describes if this source is being read from or written to.
   * @param mode The mode of the job. (implicit)
   *
   * @returns A cascading TemplateTap.
   */
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    readOrWrite match {
      case Read => throw new InvalidSourceException("Cannot use TemplateSource for input")
      case Write => {
        mode match {
          case Local(_) => {
            val localTap = new FileTap(localScheme, basePath, sinkMode)
            new LTemplateTap(localTap, template, pathFields)
          }
          case hdfsMode @ Hdfs(_, _) => {
            val hfsTap = createHfsTap(hdfsScheme, basePath, sinkMode)
            new HTemplateTap(hfsTap, template, pathFields)
          }
          case hdfsTest @ HadoopTest(_, _) => {
            val hfsTap = createHfsTap(hdfsScheme, hdfsTest.getWritePathFor(this), sinkMode)
            new HTemplateTap(hfsTap, template, pathFields)
          }
          case _ => TestTapFactory(this, hdfsScheme).createTap(readOrWrite)
        }
      }
    }
  }

  /**
   * Validates the taps, makes sure there are no nulls as the path or template.
   *
   * @param mode The mode of the job.
   */
  override def validateTaps(mode: Mode): Unit = {
    if (basePath == null) {
      throw new InvalidSourceException("basePath cannot be null for TemplateTap")
    } else if (template == null) {
      throw new InvalidSourceException("template cannot be null for TemplateTap")
    }
  }
}

/**
 * An implementation of TSV output, split over a template tap.
 *
 * @param basePath The root path for the output.
 * @param template The java formatter style string to use as the template. e.g. %s/%s.
 * @param pathFields The set of fields to apply to the path.
 * @param writeHeader Flag to indicate that the header should be written to the file.
 * @param sinkMode How to handle conflicts with existing output.
 * @param fields The set of fields to apply to the output.
 */
case class TemplatedTsv(
  override val basePath: String,
  override val template: String,
  override val pathFields: Fields = Fields.ALL,
  override val writeHeader: Boolean = false,
  override val sinkMode: SinkMode = SinkMode.REPLACE,
  override val fields: Fields = Fields.ALL)
  extends TemplateSource with DelimitedScheme

/**
 * An implementation of SequenceFile output, split over a template tap.
 *
 * @param basePath The root path for the output.
 * @param template The java formatter style string to use as the template. e.g. %s/%s.
 * @param sequenceFields The set of fields to use for the sequence file.
 * @param pathFields The set of fields to apply to the path.
 * @param sinkMode How to handle conflicts with existing output.
 */
case class TemplatedSequenceFile(
  override val basePath: String,
  override val template: String,
  val sequenceFields: Fields = Fields.ALL,
  override val pathFields: Fields = Fields.ALL,
  override val sinkMode: SinkMode = SinkMode.REPLACE)
  extends TemplateSource with SequenceFileScheme {

  override val fields = sequenceFields
}

