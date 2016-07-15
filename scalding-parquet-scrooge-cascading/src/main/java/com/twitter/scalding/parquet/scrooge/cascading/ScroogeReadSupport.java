/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.twitter.scalding.parquet.cascading.scrooge;

import org.apache.parquet.hadoop.thrift.ThriftReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.thrift.ThriftSchemaConverter;
import org.apache.parquet.thrift.projection.FieldProjectionFilter;
import org.apache.parquet.thrift.struct.ThriftType;

/**
 * Read support for Scrooge
 * @author Tianshuo Deng
 */
public class ScroogeReadSupport<T> extends ThriftReadSupport<T> {

  /**
   * used from hadoop
   * the configuration must contain a "parquet.thrift.read.class" setting
   */
  public ScroogeReadSupport() {
  }

  @Override
  protected MessageType getProjectedSchema(FieldProjectionFilter fieldProjectionFilter) {
    ThriftType.StructType thriftStruct = new ScroogeStructConverter().convert(thriftClass);
    return new ThriftSchemaConverter(fieldProjectionFilter).convert(thriftStruct);
  }
}
