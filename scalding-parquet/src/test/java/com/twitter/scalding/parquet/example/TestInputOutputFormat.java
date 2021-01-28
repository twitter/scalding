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
package com.twitter.scalding.parquet.example;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.junit.Before;
import org.junit.Test;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.DelegatingWriteSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a clone of org.apache.parquet.hadoop.example.TestInputOutputFormat
 * from apache-parquet 1.12.0-RC1, to include the fix https://github.com/apache/parquet-mr/pull/844.
 *
 * The motivation is patching a bug while we wait for apache-parquet 1.12.0 to be published and for us to
 * update the version used. This class should be removed together with ScaldingDeprecatedInputFormat.
 */
public class TestInputOutputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(TestInputOutputFormat.class);

  final Path parquetPath = new Path("target/test/example/TestInputOutputFormat/parquet");
  final Path inputPath = new Path("src/test/java/org/apache/parquet/hadoop/example/TestInputOutputFormat.java");
  final Path outputPath = new Path("target/test/example/TestInputOutputFormat/out");
  Job writeJob;
  Job readJob;
  private String writeSchema;
  private String readSchema;
  private String partialSchema;
  private Configuration conf;

  private Class<? extends Mapper<?,?,?,?>> readMapperClass;
  private Class<? extends Mapper<?,?,?,?>> writeMapperClass;

  @Before
  public void setUp() {
    conf = new Configuration();
    writeSchema = "message example {\n" +
        "required int32 line;\n" +
        "required binary content;\n" +
        "}";

    readSchema = "message example {\n" +
        "required int32 line;\n" +
        "required binary content;\n" +
        "}";

    partialSchema = "message example {\n" +
        "required int32 line;\n" +
        "}";

    readMapperClass = ReadMapper.class;
    writeMapperClass = WriteMapper.class;
  }


  public static final class MyWriteSupport extends DelegatingWriteSupport<Group> {

    private long count = 0;

    public MyWriteSupport() {
      super(new GroupWriteSupport());
    }

    @Override
    public void write(Group record) {
      super.write(record);
      ++ count;
    }

    @Override
    public org.apache.parquet.hadoop.api.WriteSupport.FinalizedWriteContext finalizeWrite() {
      Map<String, String> extraMetadata = new HashMap<String, String>();
      extraMetadata.put("my.count", String.valueOf(count));
      return new FinalizedWriteContext(extraMetadata);
    }
  }

  public static final class MyReadSupport extends DelegatingReadSupport<Group> {
    public MyReadSupport() {
      super(new GroupReadSupport());
    }

    @Override
    public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {
      Set<String> counts = context.getKeyValueMetadata().get("my.count");
      assertTrue("counts: " + counts, counts.size() > 0);
      return super.init(context);
    }
  }

  public static class ReadMapper extends Mapper<LongWritable, Text, Void, Group> {
    private SimpleGroupFactory factory;

    protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Void, Group>.Context context) throws java.io.IOException, InterruptedException {
      factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(ContextUtil.getConfiguration(context)));
    }

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Void, Group>.Context context) throws java.io.IOException, InterruptedException {
      Group group = factory.newGroup()
          .append("line", (int) key.get())
          .append("content", value.toString());
      context.write(null, group);
    }
  }

  public static class WriteMapper extends Mapper<Void, Group, LongWritable, Text> {
    protected void map(Void key, Group value, Mapper<Void, Group, LongWritable, Text>.Context context) throws IOException, InterruptedException {
      context.write(new LongWritable(value.getInteger("line", 0)), new Text(value.getString("content", 0)));
    }
  }
  public static class PartialWriteMapper extends Mapper<Void, Group, LongWritable, Text> {
    protected void map(Void key, Group value, Mapper<Void, Group, LongWritable, Text>.Context context) throws IOException, InterruptedException {
      context.write(new LongWritable(value.getInteger("line", 0)), new Text("dummy"));
    }
  }
  private void runMapReduceJob(CompressionCodecName codec) throws IOException, ClassNotFoundException, InterruptedException {
    runMapReduceJob(codec, Collections.<String, String>emptyMap());
  }
  private void runMapReduceJob(CompressionCodecName codec, Map<String, String> extraConf) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration(this.conf);
    for (Map.Entry<String, String> entry : extraConf.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    final FileSystem fileSystem = parquetPath.getFileSystem(conf);
    fileSystem.delete(parquetPath, true);
    fileSystem.delete(outputPath, true);
    {
      writeJob = new Job(conf, "write");
      TextInputFormat.addInputPath(writeJob, inputPath);
      writeJob.setInputFormatClass(TextInputFormat.class);
      writeJob.setNumReduceTasks(0);
      ParquetOutputFormat.setCompression(writeJob, codec);
      ParquetOutputFormat.setOutputPath(writeJob, parquetPath);
      writeJob.setOutputFormatClass(ParquetOutputFormat.class);
      writeJob.setMapperClass(readMapperClass);

      ParquetOutputFormat.setWriteSupportClass(writeJob, MyWriteSupport.class);
      GroupWriteSupport.setSchema(
          MessageTypeParser.parseMessageType(writeSchema),
          writeJob.getConfiguration());
      writeJob.submit();
      waitForJob(writeJob);
    }
    {
      conf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);
      readJob = new Job(conf, "read");

      readJob.setInputFormatClass(ParquetInputFormat.class);
      ParquetInputFormat.setReadSupportClass(readJob, MyReadSupport.class);

      ParquetInputFormat.setInputPaths(readJob, parquetPath);
      readJob.setOutputFormatClass(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(readJob, outputPath);
      readJob.setMapperClass(writeMapperClass);
      readJob.setNumReduceTasks(0);
      readJob.submit();
      waitForJob(readJob);
    }
  }

  private void testReadWrite(CompressionCodecName codec) throws IOException, ClassNotFoundException, InterruptedException {
    testReadWrite(codec, Collections.<String, String>emptyMap());
  }
  private void testReadWrite(CompressionCodecName codec, Map<String, String> conf) throws IOException, ClassNotFoundException, InterruptedException {
    runMapReduceJob(codec, conf);
    final BufferedReader in = new BufferedReader(new FileReader(new File(inputPath.toString())));
    final BufferedReader out = new BufferedReader(new FileReader(new File(outputPath.toString(), "part-m-00000")));
    String lineIn;
    String lineOut = null;
    int lineNumber = 0;
    while ((lineIn = in.readLine()) != null && (lineOut = out.readLine()) != null) {
      ++lineNumber;
      lineOut = lineOut.substring(lineOut.indexOf("\t") + 1);
      assertEquals("line " + lineNumber, lineIn, lineOut);
    }
    assertNull("line " + lineNumber, out.readLine());
    assertNull("line " + lineNumber, lineIn);
    in.close();
    out.close();
  }

  @Test
  public void testReadWrite() throws IOException, ClassNotFoundException, InterruptedException {
    // TODO: Lzo requires additional external setup steps so leave it out for now
    testReadWrite(CompressionCodecName.GZIP);
    testReadWrite(CompressionCodecName.UNCOMPRESSED);
    testReadWrite(CompressionCodecName.SNAPPY);
    testReadWrite(CompressionCodecName.ZSTD);
  }

  @Test
  public void testReadWriteTaskSideMD() throws IOException, ClassNotFoundException, InterruptedException {
    testReadWrite(CompressionCodecName.UNCOMPRESSED, new HashMap<String, String>() {{ put("parquet.task.side.metadata", "true"); }});
  }

  /**
   * Uses a filter that drops all records to test handling of tasks (mappers) that need to do no work at all
   */
  @Test
  public void testReadWriteTaskSideMDAggressiveFilter() throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();

    // this filter predicate should trigger row group filtering that drops all row-groups
    ParquetInputFormat.setFilterPredicate(conf, FilterApi.eq(FilterApi.intColumn("line"), -1000));
    final String fpString = conf.get(ParquetInputFormat.FILTER_PREDICATE);

    runMapReduceJob(CompressionCodecName.UNCOMPRESSED, new HashMap<String, String>() {{
      put("parquet.task.side.metadata", "true");
      put(ParquetInputFormat.FILTER_PREDICATE, fpString);
    }});

    File file = new File(outputPath.toString(), "part-m-00000");
    List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
    assertTrue(lines.isEmpty());
  }

  @Test
  public void testReadWriteFilter() throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();

    // this filter predicate should keep some records but not all (first 500 characters)
    // "line" is actually position in the file...
    ParquetInputFormat.setFilterPredicate(conf, FilterApi.lt(FilterApi.intColumn("line"), 500));
    final String fpString = conf.get(ParquetInputFormat.FILTER_PREDICATE);

    runMapReduceJob(CompressionCodecName.UNCOMPRESSED, new HashMap<String, String>() {{
      put("parquet.task.side.metadata", "true");
      put(ParquetInputFormat.FILTER_PREDICATE, fpString);
    }});

    File file = new File(inputPath.toString());
    List<String> expected = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);

    // grab the lines that contain the first 500 characters (including the rest of the line past 500 characters)
    int size = 0;
    Iterator<String> iter = expected.iterator();
    while(iter.hasNext()) {
      String next = iter.next();

      if (size < 500) {
        size += next.length();
        continue;
      }

      iter.remove();
    }

    // put the output back into it's original format (remove the character counts / tabs)
    File file2 = new File(outputPath.toString(), "part-m-00000");
    List<String> found = Files.readAllLines(file2.toPath(), StandardCharsets.UTF_8);
    StringBuilder sbFound = new StringBuilder();
    for (String line : found) {
      sbFound.append(line.split("\t", -1)[1]);
      sbFound.append("\n");
    }

    sbFound.deleteCharAt(sbFound.length() - 1);

    assertEquals(String.join("\n", expected), sbFound.toString());
  }

  @Test
  public void testProjection() throws Exception{
    readSchema=partialSchema;
    writeMapperClass = PartialWriteMapper.class;
    runMapReduceJob(CompressionCodecName.GZIP);
  }

  private static long value(Job job, String groupName, String name) throws Exception {
    // getGroup moved to AbstractCounters
    Method getGroup = org.apache.hadoop.mapreduce.Counters.class.getMethod("getGroup", String.class);
    // CounterGroup changed to an interface
    Method findCounter = org.apache.hadoop.mapreduce.CounterGroup.class.getMethod("findCounter", String.class);
    // Counter changed to an interface
    Method getValue = org.apache.hadoop.mapreduce.Counter.class.getMethod("getValue");
    CounterGroup group = (CounterGroup) getGroup.invoke(job.getCounters(), groupName);
    Counter counter = (Counter) findCounter.invoke(group, name);
    return (Long) getValue.invoke(counter);
  }

  @Test
  public void testReadWriteWithCounter() throws Exception {
    runMapReduceJob(CompressionCodecName.GZIP);

    assertTrue(value(readJob, "parquet", "bytesread") > 0L);
    assertTrue(value(readJob, "parquet", "bytestotal") > 0L);
    assertTrue(value(readJob, "parquet", "bytesread")
        == value(readJob, "parquet", "bytestotal"));
    //not testing the time read counter since it could be zero due to the size of data is too small
  }

  @Test
  public void testReadWriteWithoutCounter() throws Exception {
    conf.set("parquet.benchmark.time.read", "false");
    conf.set("parquet.benchmark.bytes.total", "false");
    conf.set("parquet.benchmark.bytes.read", "false");
    runMapReduceJob(CompressionCodecName.GZIP);
    assertTrue(value(readJob, "parquet", "bytesread") == 0L);
    assertTrue(value(readJob, "parquet", "bytestotal") == 0L);
    assertTrue(value(readJob, "parquet", "timeread") == 0L);
  }

  private void waitForJob(Job job) throws InterruptedException, IOException {
    while (!job.isComplete()) {
      LOG.debug("waiting for job {}", job.getJobName());
      sleep(100);
    }
    LOG.info("status for job {}: {}", job.getJobName(), (job.isSuccessful() ? "SUCCESS" : "FAILURE"));
    if (!job.isSuccessful()) {
      throw new RuntimeException("job failed " + job.getJobName());
    }
  }
}
