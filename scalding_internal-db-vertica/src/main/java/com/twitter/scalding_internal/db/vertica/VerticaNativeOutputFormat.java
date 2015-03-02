package com.twitter.scalding_internal.db.vertica;

import java.io.OutputStream;
import java.io.IOException;

import org.apache.hadoop.mapred.FileOutputFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

/**
 * Base class for Lzo outputformats.
 * provides an helper method to create lzo output stream.
 */
public class VerticaNativeOutputFormat extends FileOutputFormat<NullWritable, VerticaRowWrapper> {

@Override
public RecordWriter<NullWritable, VerticaRowWrapper> getRecordWriter(FileSystem ignored,
                                                                      JobConf job,
                                                                      String name,
                                                                      Progressable progress) throws IOException {

    Path file = FileOutputFormat.getTaskOutputPath(job, name);
    FileSystem fs = file.getFileSystem(job);
    FSDataOutputStream fileOut = fs.create(file, progress);

    OutputStream os = VerticaUtils.getOutputStream(job, fileOut);

    return new VerticaRawWriter(os);
  }
}
