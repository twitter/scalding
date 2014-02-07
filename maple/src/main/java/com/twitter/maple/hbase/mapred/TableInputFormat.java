/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.maple.hbase.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapred.TableInputFormatBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.StringUtils;

/**
 * Convert HBase tabular data into a format that is consumable by Map/Reduce.
 */
public class TableInputFormat extends TableInputFormatBase implements
    JobConfigurable {
  private final Log LOG = LogFactory.getLog(TableInputFormat.class);

  /**
   * space delimited list of columns
   */
  public static final String COLUMN_LIST = "hbase.mapred.tablecolumns";

  /**
   * Use this jobconf param to specify the input table
   */
  public static final String INPUT_TABLE = "hbase.mapred.inputtable";

  public void configure(JobConf job) {
    String tableName = TableInputFormat.getTableName(job);
    String colArg = job.get(COLUMN_LIST);
    String[] colNames = colArg.split(" ");
    byte [][] m_cols = new byte[colNames.length][];
    for (int i = 0; i < m_cols.length; i++) {
      m_cols[i] = Bytes.toBytes(colNames[i]);
    }
    setInputColumns(m_cols);
    try {
      setHTable(new HTable(HBaseConfiguration.create(job), tableName));
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  public void validateInput(JobConf job) throws IOException {
    // expecting exactly one path
    String tableName = TableInputFormat.getTableName(job);
    if (tableName == null) {
      throw new IOException("expecting one table name");
    }

    // connected to table?
    if (getHTable() == null) {
      throw new IOException("could not connect to table '" +
        tableName + "'");
    }

    // expecting at least one column
    String colArg = job.get(COLUMN_LIST);
    if (colArg == null || colArg.length() == 0) {
      throw new IOException("expecting at least one column");
    }
  }

  public static void setTableName(JobConf job, String tableName) {
    // Make sure that table has not been set before
    String oldTableName = getTableName(job);
    if(oldTableName != null) {
      throw new RuntimeException("table name already set to: '"
        + oldTableName + "'");
    }
    job.set(INPUT_TABLE, tableName);
  }

  public static String getTableName(JobConf job) {
    return job.get(INPUT_TABLE);
  }
}
