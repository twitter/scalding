package com.twitter.scalding.tap;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.scheme.Scheme;
import cascading.tap.hadoop.Hfs;

/**
 * Default implementation of getSize in {@link Hfs} don't respect to paths with glob patterns,
 * that will throw IOException where we actually can calculate size of source.
 */
public class GlobHfs extends Hfs {
  public GlobHfs(Scheme<JobConf, RecordReader, OutputCollector, ?, ?> scheme) {
    super(scheme);
  }

  public GlobHfs(Scheme<JobConf, RecordReader, OutputCollector, ?, ?> scheme, String stringPath) {
    super(scheme, stringPath);
  }

  @Override
  public long getSize(JobConf conf) throws IOException {
    return getSize(getPath(), conf);
  }

  /**
   * Get the total size of the file(s) specified by the Hfs, which may contain a glob
   * pattern in its path, so we must be ready to handle that case.
   */
  public static long getSize(Path path, JobConf conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    FileStatus[] statuses = fs.globStatus(path);

    if (statuses == null) {
      throw new FileNotFoundException(String.format("File not found: %s", path));
    }

    long size = 0;
    for (FileStatus status : statuses) {
      size += fs.getContentSummary(status.getPath()).getLength();
    }
    return size;
  }
}
