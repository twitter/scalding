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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import com.twitter.scrooge.ThriftStruct;
import com.twitter.scrooge.ThriftStructCodec;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.thrift.ThriftReader;
import org.apache.parquet.thrift.ThriftRecordConverter;
import org.apache.parquet.thrift.struct.ThriftType.StructType;

public class ScroogeRecordConverter<T extends ThriftStruct> extends ThriftRecordConverter<T> {


  public ScroogeRecordConverter(final Class<T> thriftClass, MessageType parquetSchema, StructType thriftType) {
    super(new ThriftReader<T>() {
      @SuppressWarnings("unchecked")
      ThriftStructCodec<T> codec = (ThriftStructCodec<T>) getCodec(thriftClass);
      @Override
      public T readOneRecord(TProtocol protocol) throws TException {
          return codec.decode(protocol);
      }
    }, thriftClass.getSimpleName(), parquetSchema, thriftType);
  }

  private static ThriftStructCodec<?> getCodec(Class<?> klass) {
    Class<?> companionClass;
    try {
      companionClass = Class.forName(klass.getName() + "$");
      Object companionObject = companionClass.getField("MODULE$").get(null);
      return (ThriftStructCodec<?>) companionObject;
    } catch (Exception t) {
      if (t instanceof InterruptedException) Thread.currentThread().interrupt();
      throw new RuntimeException("Unable to create ThriftStructCodec", t);
    }
  }
}
