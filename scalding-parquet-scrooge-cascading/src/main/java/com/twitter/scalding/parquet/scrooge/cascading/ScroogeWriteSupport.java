/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.scalding.parquet.cascading.scrooge;

import com.twitter.scrooge.ThriftStruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;

import org.apache.parquet.hadoop.thrift.AbstractThriftWriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.thrift.struct.ThriftType.StructType;

/**
 * Write support for Scrooge
 */
public class ScroogeWriteSupport<T extends ThriftStruct> extends AbstractThriftWriteSupport<T> {
  public static void setScroogeClass(Configuration configuration, Class<? extends ThriftStruct> thriftClass) {
    AbstractThriftWriteSupport.setGenericThriftClass(configuration, thriftClass);
  }

  public static Class<? extends ThriftStruct> getScroogeClass(Configuration configuration) {
    return (Class<? extends ThriftStruct>)AbstractThriftWriteSupport.getGenericThriftClass(configuration);
  }

  /**
   * used from hadoop
   * the configuration must contain a "parquet.thrift.write.class" setting
   * (see ScroogeWriteSupport#setScroogeClass)
   */
  public ScroogeWriteSupport() {
  }

  public ScroogeWriteSupport(Class<T> thriftClass) {
    super(thriftClass);
  }

  @Override
  protected StructType getThriftStruct() {
    ScroogeStructConverter schemaConverter = new ScroogeStructConverter();
    return schemaConverter.convert(thriftClass);
  }

  @Override
  public void write(T record) {
    try {
      record.write(parquetWriteProtocol);
    } catch (TException e) {
      throw new ParquetEncodingException(e);
    }
  }
}
