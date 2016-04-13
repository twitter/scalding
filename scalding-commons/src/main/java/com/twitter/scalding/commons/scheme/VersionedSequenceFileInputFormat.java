package com.twitter.scalding.commons.scheme;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

/**
 * Hadoop's SequenceFileInputFormat assumes separate "data" and index" files per directory.
 * This does not apply to VersionedKeyValSource, so we bypass that behavior.
 */
public class VersionedSequenceFileInputFormat<K, V> extends FileInputFormat<K, V> {

  public VersionedSequenceFileInputFormat() {
    setMinSplitSize(SequenceFile.SYNC_INTERVAL);
  }

  private final PathFilter hiddenPathFilter = new PathFilter() {
    // avoid hidden files and directories.
    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      return !name.startsWith(".") && !name.startsWith("_");
    }
  };

  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    // we pick all the parent directories (should be only one for the picked version)
    // and fetch the part files (non-hidden) under them
    // any files in the parent list are version files which are to be disregarded
    FileStatus[] parentPaths = super.listStatus(job);
    List<FileStatus> result = new ArrayList<FileStatus>();
    for (int i = 0; i < parentPaths.length; i++) {
      FileStatus status = parentPaths[i];
      if (status.isDirectory()) {
        // add all files under this dir
        FileSystem fs = status.getPath().getFileSystem(job);
        result.addAll(Arrays.asList(fs.listStatus(status.getPath(), hiddenPathFilter)));
      }
    }
    return result.toArray(new FileStatus[0]);
  }

  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    reporter.setStatus(split.toString());
    return new SequenceFileRecordReader<K, V>(job, (FileSplit) split);
  }
}

