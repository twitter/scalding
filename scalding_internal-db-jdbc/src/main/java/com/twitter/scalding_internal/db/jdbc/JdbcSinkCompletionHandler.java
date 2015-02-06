package com.twitter.scalding_internal.db.jdbc;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;

public interface JdbcSinkCompletionHandler {
  boolean commitResource(JobConf conf, String path) throws IOException;
}
