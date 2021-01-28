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
package com.twitter.scalding.parquet;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.junit.Before;
import org.junit.Test;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.mapred.Container;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class is a clone of org.apache.parquet.hadoop.DeprecatedInputFormatTest
 * from apache-parquet 1.12.0-RC1, to include the fix https://github.com/apache/parquet-mr/pull/844.
 *
 * The motivation is patching a bug while we wait for apache-parquet 1.12.0 to be published and for us to
 * update the version used. This class should be removed together with ScaldingDeprecatedInputFormat.
 *
 * ScaldingDeprecatedParquetInputFormat is used by cascading. It initializes the recordReader using an initialize method with
 * different parameters than ParquetInputFormat
 */
public class ScaldingDeprecatedInputFormatTest {
  final Path parquetPath = new Path("target/test/example/TestInputOutputFormat/parquet");
  final Path inputPath = new Path("src/test/java/com/twitter/scalding/parquet/example/TestInputOutputFormat.java");
  final Path outputPath = new Path("target/test/example/TestInputOutputFormat/out");
  Job writeJob;
  JobConf jobConf;
  RunningJob mapRedJob;
  private String writeSchema;
  private String readSchema;
  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
    jobConf = new JobConf();
    writeSchema = "message example {\n" +
        "required int32 line;\n" +
        "required binary content;\n" +
        "}";

    readSchema = "message example {\n" +
        "required int32 line;\n" +
        "required binary content;\n" +
        "}";
  }

  private void runMapReduceJob(CompressionCodecName codec) throws IOException, ClassNotFoundException, InterruptedException {

    final FileSystem fileSystem = parquetPath.getFileSystem(conf);
    fileSystem.delete(parquetPath, true);
    fileSystem.delete(outputPath, true);
    {
      writeJob = new Job(conf, "write");
      TextInputFormat.addInputPath(writeJob, inputPath);
      writeJob.setInputFormatClass(TextInputFormat.class);
      writeJob.setNumReduceTasks(0);
      ExampleOutputFormat.setCompression(writeJob, codec);
      ExampleOutputFormat.setOutputPath(writeJob, parquetPath);
      writeJob.setOutputFormatClass(ExampleOutputFormat.class);
      writeJob.setMapperClass(ReadMapper.class);
      ExampleOutputFormat.setSchema(
          writeJob,
          MessageTypeParser.parseMessageType(
              writeSchema));
      writeJob.submit();
      waitForJob(writeJob);
    }
    {
      jobConf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);
      jobConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, GroupReadSupport.class.getCanonicalName());
      jobConf.setInputFormat(MyDeprecatedInputFormat.class);
      MyDeprecatedInputFormat.setInputPaths(jobConf, parquetPath);
      jobConf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);
      org.apache.hadoop.mapred.TextOutputFormat.setOutputPath(jobConf, outputPath);
      jobConf.setMapperClass(DeprecatedWriteMapper.class);
      jobConf.setNumReduceTasks(0);
      mapRedJob = JobClient.runJob(jobConf);
    }
  }

  // This is the RecordReader implementation simulate cascading 2 behavior:
  // https://github.com/Cascading/cascading/blob/2.6/cascading-hadoop/
  // src/main/shared/cascading/tap/hadoop/io/CombineFileRecordReaderWrapper.java
  static class CombineFileRecordReaderWrapper<K, V> implements RecordReader<K, V>
  {
    private final RecordReader<K, V> delegate;

    public CombineFileRecordReaderWrapper( CombineFileSplit split, Configuration conf, Reporter reporter, Integer idx ) throws Exception
    {
      FileSplit fileSplit = new FileSplit(
          split.getPath( idx ),
          split.getOffset( idx ),
          split.getLength( idx ),
          split.getLocations()
      );

      delegate = new ScaldingDeprecatedParquetInputFormat().getRecordReader( fileSplit, (JobConf) conf, reporter );
    }

    public boolean next( K key, V value ) throws IOException
    {
      return delegate.next( key, value );
    }

    public K createKey()
    {
      return delegate.createKey();
    }

    public V createValue()
    {
      return delegate.createValue();
    }

    public long getPos() throws IOException
    {
      return delegate.getPos();
    }

    public void close() throws IOException
    {
      delegate.close();
    }

    public float getProgress() throws IOException
    {
      return delegate.getProgress();
    }
  }

  // This is the InputFormat implementation simulates cascading 2:
  // https://github.com/Cascading/cascading/blob/2.6/cascading-hadoop/
  // src/main/shared/cascading/tap/hadoop/Hfs.java#L773
  static class CombinedInputFormat extends CombineFileInputFormat implements Configurable
  {
    private Configuration conf;
    public RecordReader getRecordReader( InputSplit split, JobConf job, Reporter reporter ) throws IOException
    {
      return new CombineFileRecordReader( job, (CombineFileSplit) split, reporter, CombineFileRecordReaderWrapper.class );
    }
    @Override
    public void setConf( Configuration conf )
    {
      this.conf = conf;
    }
    @Override
    public Configuration getConf()
    {
      return conf;
    }
  }

  class PartFileFilter implements FilenameFilter {
    @Override
    public boolean accept(File dir, String name) {
      if (name.startsWith("part")) {
        return true;
      }
      return false;
    }
  }

  private File createParquetFile(String content)
      throws IOException, ClassNotFoundException, InterruptedException {
    File inputFile = File.createTempFile("temp", null);
    File outputFile = File.createTempFile("temp", null);
    outputFile.delete();
    PrintWriter pw = new PrintWriter(new FileWriter(inputFile));
    pw.println(content);
    pw.close();
    writeJob = new Job(conf, "write");

    TextInputFormat.addInputPath(writeJob, new Path(inputFile.toURI()));
    writeJob.setInputFormatClass(TextInputFormat.class);
    writeJob.setNumReduceTasks(0);
    ExampleOutputFormat.setOutputPath(writeJob, new Path(outputFile.toURI()));
    writeJob.setOutputFormatClass(ExampleOutputFormat.class);
    writeJob.setMapperClass(ReadMapper.class);
    ExampleOutputFormat.setSchema(
        writeJob,
        MessageTypeParser.parseMessageType(
            writeSchema));
    writeJob.submit();
    waitForJob(writeJob);
    File partFile = outputFile.listFiles(new PartFileFilter())[0];
    inputFile.delete();
    return partFile;
  }

  @Test
  public void testCombineParquetInputFormat() throws Exception {
    File inputDir = File.createTempFile("temp", null);
    inputDir.delete();
    inputDir.mkdirs();
    File parquetFile1 = createParquetFile("hello");
    File parquetFile2 = createParquetFile("world");
    Files.move(parquetFile1.toPath(), new File(inputDir, "1").toPath());
    Files.move(parquetFile2.toPath(), new File(inputDir, "2").toPath());

    File outputDir = File.createTempFile("temp", null);
    outputDir.delete();
    org.apache.hadoop.mapred.JobConf conf
        = new org.apache.hadoop.mapred.JobConf(ScaldingDeprecatedInputFormatTest.class);
    conf.setInputFormat(CombinedInputFormat.class);
    conf.setNumReduceTasks(0);
    conf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);
    conf.setMapperClass(DeprecatedWriteMapper.class);
    org.apache.hadoop.mapred.FileInputFormat.setInputPaths(conf, new Path(inputDir.toURI()));
    org.apache.hadoop.mapred.TextOutputFormat.setOutputPath(conf, new Path(outputDir.toURI()));
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, GroupReadSupport.class.getCanonicalName());
    JobClient.runJob(conf);
    File partFile = outputDir.listFiles(new PartFileFilter())[0];
    BufferedReader br = new BufferedReader(new FileReader(partFile));
    String line;
    Set<String> s = new HashSet<String>();
    while ((line = br.readLine()) != null) {
      s.add(line.split("\t")[1]);
    }
    assertEquals(s.size(), 2);
    assertTrue(s.contains("hello"));
    assertTrue(s.contains("world"));
    FileUtils.deleteDirectory(inputDir);
    FileUtils.deleteDirectory(outputDir);
  }

  @Test
  public void testReadWriteWithCountDeprecated() throws Exception {
    runMapReduceJob(CompressionCodecName.GZIP);
    assertTrue(mapRedJob.getCounters().getGroup("parquet").getCounterForName("bytesread").getValue() > 0L);
    assertTrue(mapRedJob.getCounters().getGroup("parquet").getCounterForName("bytestotal").getValue() > 0L);
    assertTrue(mapRedJob.getCounters().getGroup("parquet").getCounterForName("bytesread").getValue()
        == mapRedJob.getCounters().getGroup("parquet").getCounterForName("bytestotal").getValue());
    //not testing the time read counter since it could be zero due to the size of data is too small
  }

  @Test
  public void testReadWriteWithoutCounter() throws Exception {
    jobConf.set("parquet.benchmark.time.read", "false");
    jobConf.set("parquet.benchmark.bytes.total", "false");
    jobConf.set("parquet.benchmark.bytes.read", "false");
    runMapReduceJob(CompressionCodecName.GZIP);
    assertEquals(mapRedJob.getCounters().getGroup("parquet").getCounterForName("bytesread").getValue(), 0L);
    assertEquals(mapRedJob.getCounters().getGroup("parquet").getCounterForName("bytestotal").getValue(), 0L);
    assertEquals(mapRedJob.getCounters().getGroup("parquet").getCounterForName("timeread").getValue(), 0L);
  }

  private void waitForJob(Job job) throws InterruptedException, IOException {
    while (!job.isComplete()) {
      System.out.println("waiting for job " + job.getJobName());
      sleep(100);
    }
    System.out.println("status for job " + job.getJobName() + ": " + (job.isSuccessful() ? "SUCCESS" : "FAILURE"));
    if (!job.isSuccessful()) {
      throw new RuntimeException("job failed " + job.getJobName());
    }
  }

  public static class ReadMapper extends Mapper<LongWritable, Text, Void, Group> {
    private SimpleGroupFactory factory;

    protected void setup(Context context) throws IOException, InterruptedException {
      factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(ContextUtil.getConfiguration(context)));
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      Group group = factory.newGroup()
          .append("line", (int) key.get())
          .append("content", value.toString());
      context.write(null, group);
    }
  }

  public static class DeprecatedWriteMapper implements org.apache.hadoop.mapred.Mapper<Void, Container<Group>, LongWritable, Text> {

    @Override
    public void map(Void aVoid, Container<Group> valueContainer, OutputCollector<LongWritable, Text> longWritableTextOutputCollector, Reporter reporter) throws IOException {
      Group value = valueContainer.get();
      longWritableTextOutputCollector.collect(new LongWritable(value.getInteger("line", 0)), new Text(value.getString("content", 0)));
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(JobConf entries) {
    }
  }

  static class MyDeprecatedInputFormat extends ScaldingDeprecatedParquetInputFormat<Group> {

  }
}
