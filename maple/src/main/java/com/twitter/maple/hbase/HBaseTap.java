/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package com.twitter.maple.hbase;

import com.twitter.maple.hbase.mapred.TableInputFormat;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * The HBaseTap class is a {@link Tap} subclass. It is used in conjunction with
 * the {@link HBaseScheme} to allow for the reading and writing
 * of data to and from a HBase cluster.
 */
public class HBaseTap extends Tap<JobConf, RecordReader, OutputCollector> {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTap.class);

  private final String id = UUID.randomUUID().toString();

  /** Field SCHEME */
  public static final String SCHEME = "hbase";

  /** Field hBaseAdmin */
  private transient HBaseAdmin hBaseAdmin;

  /** Field hostName */
  private String quorumNames;
  /** Field tableName */
  private String tableName;

  /**
   * Constructor HBaseTap creates a new HBaseTap instance.
   *
   * @param tableName
   *          of type String
   * @param HBaseFullScheme
   *          of type HBaseFullScheme
   */
  public HBaseTap(String tableName, HBaseScheme HBaseFullScheme) {
    super(HBaseFullScheme, SinkMode.UPDATE);
    this.tableName = tableName;
  }

  /**
   * Constructor HBaseTap creates a new HBaseTap instance.
   *
   * @param tableName
   *          of type String
   * @param HBaseFullScheme
   *          of type HBaseFullScheme
   * @param sinkMode
   *          of type SinkMode
   */
  public HBaseTap(String tableName, HBaseScheme HBaseFullScheme, SinkMode sinkMode) {
    super(HBaseFullScheme, sinkMode);
    this.tableName = tableName;
  }

  /**
   * Constructor HBaseTap creates a new HBaseTap instance.
   *
   * @param quorumNames
   *          of type String
   * @param tableName
   *          of type String
   * @param HBaseFullScheme
   *          of type HBaseFullScheme
   */
  public HBaseTap(String quorumNames, String tableName, HBaseScheme HBaseFullScheme) {
    super(HBaseFullScheme, SinkMode.UPDATE);
    this.quorumNames = quorumNames;
    this.tableName = tableName;
  }

  /**
   * Constructor HBaseTap creates a new HBaseTap instance.
   *
   * @param quorumNames
   *          of type String
   * @param tableName
   *          of type String
   * @param HBaseFullScheme
   *          of type HBaseFullScheme
   * @param sinkMode
   *          of type SinkMode
   */
  public HBaseTap(String quorumNames, String tableName, HBaseScheme HBaseFullScheme, SinkMode sinkMode) {
    super(HBaseFullScheme, sinkMode);
    this.quorumNames = quorumNames;
    this.tableName = tableName;
  }

  /**
   * Method getTableName returns the tableName of this HBaseTap object.
   *
   * @return the tableName (type String) of this HBaseTap object.
   */
  public String getTableName() {
    return tableName;
  }

  public Path getPath() {
    return new Path(SCHEME + ":/" + tableName.replaceAll(":", "_"));
  }

  protected HBaseAdmin getHBaseAdmin(JobConf conf) throws MasterNotRunningException, ZooKeeperConnectionException {
    if (hBaseAdmin == null) {
      Configuration hbaseConf = HBaseConfiguration.create(conf);
      hBaseAdmin = new HBaseAdmin(hbaseConf);
    }

    return hBaseAdmin;
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
    if (quorumNames != null) {
      conf.set("hbase.zookeeper.quorum", quorumNames);
    } else {
      Configuration hbaseConfig = HBaseConfiguration.create(conf);
      conf.set(HConstants.ZOOKEEPER_QUORUM, hbaseConfig.get(HConstants.ZOOKEEPER_QUORUM));
      conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, hbaseConfig.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    }

    LOG.debug("sinking to table: {}", tableName);

    if (isReplace() && conf.get("mapred.task.partition") == null) {
      try {
        deleteResource(conf);

      } catch (IOException e) {
        throw new RuntimeException("could not delete resource: " + e);
      }
    }

    else if (isUpdate()) {
      try {
          createResource(conf);
      } catch (IOException e) {
          throw new RuntimeException(tableName + " does not exist !", e);
      }

    }

    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
    super.sinkConfInit(process, conf);
  }

  @Override
  public String getIdentifier() {
    return id;
  }

  @Override
  public TupleEntryIterator openForRead(FlowProcess<JobConf> jobConfFlowProcess, RecordReader recordReader) throws IOException {
    return new HadoopTupleEntrySchemeIterator(jobConfFlowProcess, this, recordReader);
  }

  @Override
  public TupleEntryCollector openForWrite(FlowProcess<JobConf> jobConfFlowProcess, OutputCollector outputCollector) throws IOException {
    HBaseTapCollector hBaseCollector = new HBaseTapCollector( jobConfFlowProcess, this );
    hBaseCollector.prepare();
    return hBaseCollector;
  }

  @Override
  public boolean createResource(JobConf jobConf) throws IOException {
    HBaseAdmin hBaseAdmin = getHBaseAdmin(jobConf);

    if (hBaseAdmin.tableExists(tableName)) {
      return true;
    }

    LOG.info("creating hbase table: {}", tableName);

    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

    String[] familyNames = ((HBaseScheme) getScheme()).getFamilyNames();

    for (String familyName : familyNames) {
      tableDescriptor.addFamily(new HColumnDescriptor(familyName));
    }

    hBaseAdmin.createTable(tableDescriptor);

    return true;
  }

  @Override
  public boolean deleteResource(JobConf jobConf) throws IOException {
    // TODO: for now we don't do anything just to be safe
    return true;
  }

  @Override
  public boolean resourceExists(JobConf jobConf) throws IOException {
    return getHBaseAdmin(jobConf).tableExists(tableName);
  }

  @Override
  public long getModifiedTime(JobConf jobConf) throws IOException {
    return System.currentTimeMillis(); // currently unable to find last mod time
                                       // on a table
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf) {
    // a hack for MultiInputFormat to see that there is a child format
    FileInputFormat.setInputPaths( conf, getPath() );

    if(quorumNames != null) {
      conf.set("hbase.zookeeper.quorum", quorumNames);
    }

    LOG.debug("sourcing from table: {}", tableName);
    TableInputFormat.setTableName(conf, tableName);
    super.sourceConfInit(process, conf);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }

    HBaseTap hBaseTap = (HBaseTap) object;

    if (tableName != null ? !tableName.equals(hBaseTap.tableName) : hBaseTap.tableName != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
    return result;
  }
}
