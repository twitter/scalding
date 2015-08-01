/**
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
package com.twitter.scalding.parquet.scrooge;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import com.twitter.scalding.parquet.scrooge.thrift_scala.test.StringAndBinary;
import org.apache.parquet.thrift.ThriftParquetReader;

public class ScroogeBinaryTest {
  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testScroogeBinaryEncoding() throws Exception {
    StringAndBinary expected = new StringAndBinary.Immutable("test",
        ByteBuffer.wrap(new byte[] {-123, 20, 33}));

    File temp = tempDir.newFile(UUID.randomUUID().toString());
    temp.deleteOnExit();
    temp.delete();

    Path path = new Path(temp.getPath());

    ParquetWriter<StringAndBinary> writer = new ParquetWriter<StringAndBinary>(
        path, new Configuration(), new ScroogeWriteSupport<StringAndBinary>(StringAndBinary.class));
    writer.write(expected);
    writer.close();

    // read using the parquet-thrift version to isolate the write path
    ParquetReader<com.twitter.scalding.parquet.scrooge.thrift_java.test.binary.StringAndBinary> reader = ThriftParquetReader.<com.twitter.scalding.parquet.scrooge.thrift_java.test.binary.StringAndBinary>
        build(path)
        .withThriftClass(com.twitter.scalding.parquet.scrooge.thrift_java.test.binary.StringAndBinary.class)
        .build();
    com.twitter.scalding.parquet.scrooge.thrift_java.test.binary.StringAndBinary record = reader.read();
    reader.close();

    Assert.assertEquals("String should match after serialization round trip",
        "test", record.s);
    Assert.assertEquals("ByteBuffer should match after serialization round trip",
        ByteBuffer.wrap(new byte[] {-123, 20, 33}), record.b);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testScroogeBinaryDecoding() throws Exception {
    StringAndBinary expected = new StringAndBinary.Immutable("test",
        ByteBuffer.wrap(new byte[] {-123, 20, 33}));

    File temp = tempDir.newFile(UUID.randomUUID().toString());
    temp.deleteOnExit();
    temp.delete();

    Path path = new Path(temp.getPath());

    ParquetWriter<StringAndBinary> writer = new ParquetWriter<StringAndBinary>(
        path, new Configuration(), new ScroogeWriteSupport<StringAndBinary>(StringAndBinary.class));
    writer.write(expected);
    writer.close();

    Configuration conf = new Configuration();
    conf.set("parquet.thrift.converter.class", ScroogeRecordConverter.class.getName());
    ParquetReader<StringAndBinary> reader = ParquetReader.<StringAndBinary>
        builder(new ScroogeReadSupport(), path)
        .withConf(conf)
        .build();
    StringAndBinary record = reader.read();
    reader.close();

    Assert.assertEquals("String should match after serialization round trip",
        "test", record.s());
    Assert.assertEquals("ByteBuffer should match after serialization round trip",
        ByteBuffer.wrap(new byte[] {-123, 20, 33}), record.b());
  }
}
