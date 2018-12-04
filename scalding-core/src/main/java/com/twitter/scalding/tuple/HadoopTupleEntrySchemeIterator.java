/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.scalding.tuple;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.MultiInputSplit;
import cascading.tap.hadoop.io.MultiRecordReaderIterator;
import cascading.tap.hadoop.io.RecordReaderIterator;
import cascading.tap.hadoop.util.MeasuredRecordReader;
import cascading.util.CloseableIterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 *
 */
public class HadoopTupleEntrySchemeIterator extends TupleEntrySchemeIterator<JobConf, RecordReader>
{
  private MeasuredRecordReader measuredRecordReader;

  public HadoopTupleEntrySchemeIterator( FlowProcess<JobConf> flowProcess, Tap parentTap, RecordReader recordReader ) throws IOException
  {
    this( flowProcess, parentTap.getScheme(), makeIterator( flowProcess, parentTap, recordReader ) );
  }

  public HadoopTupleEntrySchemeIterator( FlowProcess<JobConf> flowProcess, Scheme scheme, CloseableIterator<RecordReader> closeableIterator )
  {
    super( flowProcess, scheme, closeableIterator, flowProcess.getStringProperty( MultiInputSplit.CASCADING_SOURCE_PATH ) );
  }

  private static CloseableIterator<RecordReader> makeIterator( FlowProcess<JobConf> flowProcess, Tap parentTap, RecordReader recordReader ) throws IOException
  {
    if( recordReader != null )
      return new RecordReaderIterator( recordReader );

    return new MultiRecordReaderIterator( flowProcess, parentTap );
  }

  @Override
  protected RecordReader wrapInput( RecordReader recordReader )
  {
    if( measuredRecordReader == null )
      measuredRecordReader = new MeasuredRecordReader( getFlowProcess(), SliceCounters.Read_Duration );

    measuredRecordReader.setRecordReader( recordReader );

    return measuredRecordReader;
  }
}
