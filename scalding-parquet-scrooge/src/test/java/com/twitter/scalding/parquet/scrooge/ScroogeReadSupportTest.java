package com.twitter.scalding.parquet.scrooge;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.twitter.scalding.parquet.scrooge.thrift_scala.test.Address;
import com.twitter.scalding.parquet.tuple.TupleWriteSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.thrift.ThriftReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.thrift.ThriftSchemaConverter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.UUID;

public class ScroogeReadSupportTest {
  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testReadParquetWithoutThriftMetadata() throws Exception {
    File temp = tempDir.newFile(UUID.randomUUID().toString());
    temp.deleteOnExit();
    temp.delete();
    Path path = new Path(temp.getPath());

    Address expected = new Address.Immutable("street1", "zip1");
    // Corresponding tuple entry for the above object
    TupleEntry entry = new TupleEntry(new Fields("street", "zip"), new Tuple(expected.street(), expected.zip()));
    // Getting corresponding MessageType from the Address thrift
    MessageType schema = new ThriftSchemaConverter().convert(new ScroogeStructConverter().convert(Address.class));

    Configuration conf = new Configuration();
    conf.set(TupleWriteSupport.PARQUET_CASCADING_SCHEMA, schema.toString());

    // Writing using TupleWriter, this does not add metadata to the parquet
    ParquetWriter writer = new ParquetWriter(path, conf, new TupleWriteSupport());
    writer.write(entry);
    writer.close();

    conf.set(ThriftReadSupport.THRIFT_READ_CLASS_KEY, Address.class.getName());
    // Reading using ScroogeReadSupport
    ParquetReader<Address> reader = ParquetReader.<Address>
      builder(new ScroogeReadSupport(), path)
      .withConf(conf)
      .build();
    Address record = reader.read();
    reader.close();

    Assert.assertEquals(record, expected);
  }
}
