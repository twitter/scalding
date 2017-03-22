package com.twitter.scalding.commons.tap;

import java.io.IOException;

import com.twitter.scalding.commons.datastores.VersionedStore;
import com.twitter.scalding.tap.GlobHfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;

public class VersionedTap extends GlobHfs {
  public static enum TapMode {SOURCE, SINK}

  public Long version = null;

  // a sane default for the number of versions of your data to keep around
  private int versionsToKeep = 3;

  // source-specific
  public TapMode mode;

  // sink-specific
  private String newVersionPath;

  public VersionedTap(String dir, Scheme<JobConf,RecordReader,OutputCollector,?,?> scheme, TapMode mode)
      throws IOException {
    super(scheme, dir);
    this.mode = mode;
  }


  public VersionedTap setVersion(long version) {
    this.version = version;
    return this;
  }

  /**
    * Sets the number of versions of your data to keep. Unneeded versions are cleaned up on creation
    * of a new one. Pass a negative number to keep all versions.
    */
  public VersionedTap setVersionsToKeep(int versionsToKeep) {
    this.versionsToKeep = versionsToKeep;
    return this;
  }

  public int getVersionsToKeep() {
    return this.versionsToKeep;
  }

  public String getOutputDirectory() {
    return getPath().toString();
  }

  public VersionedStore getStore(JobConf conf) throws IOException {
    return new VersionedStore(getPath().getFileSystem(conf), getOutputDirectory());
  }

  public String getSourcePath(JobConf conf) {
    VersionedStore store;
    try {
      store = getStore(conf);
      String sourcePath = (version != null) ? store.versionPath(version) : store.mostRecentVersionPath();
      if (sourcePath == null) {
        throw new RuntimeException("Could not find valid source path for VersionTap with root: " + store.getRoot());
      }
      return sourcePath;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String getSinkPath(JobConf conf) {
    try {
      VersionedStore store = getStore(conf);
      String sinkPath = (version == null) ? store.createVersion() : store.createVersion(version);
      if (sinkPath == null) {
        throw new RuntimeException("Could not find valid sink path for VersionTap with root: " + store.getRoot());
      }
      return sinkPath;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf) {
    super.sourceConfInit(process, conf);
    FileInputFormat.setInputPaths(conf, getSourcePath(conf));
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
    super.sinkConfInit(process, conf);

    if (newVersionPath == null)
      newVersionPath = getSinkPath(conf);

    FileOutputFormat.setOutputPath(conf, new Path(newVersionPath));
  }

  @Override
  public long getSize(JobConf conf) throws IOException {
    return getSize(new Path(getSourcePath(conf)), conf);
  }

  @Override
  public boolean resourceExists(JobConf jc) throws IOException {
    return getStore(jc).mostRecentVersion() != null;
  }

  @Override
  public boolean createResource(JobConf jc) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean deleteResource(JobConf jc) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getIdentifier() {
    String outDir = getOutputDirectory();
    String versionString = (version == null) ? "LATEST" : version.toString();
    return outDir + Path.SEPARATOR
           + ((mode == TapMode.SINK) ? "sink" : "source")
           + Path.SEPARATOR + versionString;
  }

  @Override
  public long getModifiedTime(JobConf conf) throws IOException {
    VersionedStore store = getStore(conf);
    return (mode == TapMode.SINK) ? 0 : store.mostRecentVersion();
  }

  @Override
  public boolean commitResource(JobConf conf) throws IOException {
    VersionedStore store = getStore(conf);

    if (newVersionPath != null) {
      store.succeedVersion(newVersionPath);
      markSuccessfulOutputDir(new Path(newVersionPath), conf);
      newVersionPath = null;
      store.cleanup(getVersionsToKeep());
    }

    return true;
  }

  private static void markSuccessfulOutputDir(Path path, JobConf conf) throws IOException {
      FileSystem fs = path.getFileSystem(conf);
      // create a file in the folder to mark it
      if (fs.exists(path)) {
          Path filePath = new Path(path, VersionedStore.HADOOP_SUCCESS_FLAG);
          fs.create(filePath).close();
      }
  }

  @Override
  public boolean rollbackResource(JobConf conf) throws IOException {
    if (newVersionPath != null) {
      getStore(conf).failVersion(newVersionPath);
      newVersionPath = null;
    }

    return true;
  }
}
