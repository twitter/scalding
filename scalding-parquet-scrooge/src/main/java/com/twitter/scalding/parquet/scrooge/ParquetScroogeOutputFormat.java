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
package com.twitter.scalding.parquet.scrooge;

import com.twitter.scalding.parquet.cascading.scrooge.ScroogeWriteSupport;
import com.twitter.scrooge.ThriftStruct;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetOutputFormat;

/**
 * Use this class to write Scrooge records to parquet
 * @param <T>  Type of Scrooge records to write
 */
public class ParquetScroogeOutputFormat<T extends ThriftStruct> extends ParquetOutputFormat<T> {

  public static void setScroogeClass(Configuration configuration, Class<? extends ThriftStruct> thriftClass) {
    ScroogeWriteSupport.setScroogeClass(configuration, thriftClass);
  }

  public static Class<? extends ThriftStruct> getScroogeClass(Configuration configuration) {
    return ScroogeWriteSupport.getScroogeClass(configuration);
  }

  public ParquetScroogeOutputFormat() {
    super(new ScroogeWriteSupport<T>());
  }
}
