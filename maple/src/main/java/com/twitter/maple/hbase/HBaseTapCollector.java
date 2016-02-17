/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.maple.hbase;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntrySchemeCollector;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Class HBaseTapCollector is a kind of
 * {@link cascading.tuple.TupleEntrySchemeCollector} that writes tuples to the
 * resource managed by a particular {@link HBaseTap} instance.
 */
public class HBaseTapCollector extends TupleEntrySchemeCollector<JobConf, TupleEntrySchemeCollector> implements OutputCollector {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTapCollector.class);
  /** Field conf */
  private final JobConf conf;
  /** Field writer */
  private RecordWriter writer;
  /** Field flowProcess */
  private final FlowProcess<? extends JobConf> hadoopFlowProcess;
  /** Field tap */
  private final Tap<JobConf, RecordReader, OutputCollector> tap;
  /** Field reporter */
  private final Reporter reporter = Reporter.NULL;

  /**
   * Constructor TapCollector creates a new TapCollector instance.
   *
   * @param flowProcess
   *          of type FlowProcess
   * @param tap
   *          of type Tap
   * @throws IOException
   *           when fails to initialize
   */
  public HBaseTapCollector(FlowProcess<? extends JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap) throws IOException {
    super(flowProcess, tap.getScheme());
    this.hadoopFlowProcess = flowProcess;
    this.tap = tap;
    this.conf = new JobConf(flowProcess.getConfigCopy());
    this.setOutput(this);
  }

  @Override
  public void prepare() {
    try {
      initialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    super.prepare();
  }

  private void initialize() throws IOException {
    tap.sinkConfInit(hadoopFlowProcess, conf);
    OutputFormat outputFormat = conf.getOutputFormat();
    LOG.info("Output format class is: " + outputFormat.getClass().toString());
    writer = outputFormat.getRecordWriter(null, conf, tap.getIdentifier(), Reporter.NULL);
    sinkCall.setOutput(this);
  }

  @Override
  public void close() {
    try {
      LOG.info("closing tap collector for: {}", tap);
      writer.close(reporter);
    } catch (IOException exception) {
      LOG.warn("exception closing: {}", exception);
      throw new TapException("exception closing HBaseTapCollector", exception);
    } finally {
      super.close();
    }
  }

  /**
   * Method collect writes the given values to the {@link Tap} this instance
   * encapsulates.
   *
   * @param writableComparable
   *          of type WritableComparable
   * @param writable
   *          of type Writable
   * @throws IOException
   *           when
   */
  public void collect(Object writableComparable, Object writable) throws IOException {
    if (hadoopFlowProcess instanceof HadoopFlowProcess)
      ((HadoopFlowProcess) hadoopFlowProcess).getReporter().progress();

    writer.write(writableComparable, writable);
  }
}
