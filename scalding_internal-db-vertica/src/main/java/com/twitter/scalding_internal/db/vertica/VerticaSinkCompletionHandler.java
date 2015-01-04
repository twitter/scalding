package com.twitter.scalding_internal.db.vertica;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;

interface VerticaSinkCompletionHandler {
  boolean commitResource(JobConf conf, String path) throws IOException;
}
