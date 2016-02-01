package com.twitter.scalding.parquet.tuple;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.commons.lang.StringUtils;

import cascading.tuple.Tuple;
import cascading.tuple.Fields;

import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;


public class TupleReadSupport extends ReadSupport<Tuple> {
  static final String PARQUET_CASCADING_REQUESTED_FIELDS = "parquet.cascading.requested.fields";

  static protected Fields getRequestedFields(Configuration configuration) {
    String fieldsString = configuration.get(PARQUET_CASCADING_REQUESTED_FIELDS);

    if(fieldsString == null)
      return Fields.ALL;

    String[] parts = StringUtils.split(fieldsString, ":");
    if(parts.length == 0)
      return Fields.ALL;
    else
      return new Fields(parts);
  }

  static protected void setRequestedFields(JobConf configuration, Fields fields) {
    String fieldsString = StringUtils.join(fields.iterator(), ":");
    configuration.set(PARQUET_CASCADING_REQUESTED_FIELDS, fieldsString);
  }

  @Override
  public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    Fields requestedFields = getRequestedFields(configuration);
    if (requestedFields == null) {
      return new ReadContext(fileSchema);
    } else {
      SchemaIntersection intersection = new SchemaIntersection(fileSchema, requestedFields);
      return new ReadContext(intersection.getRequestedSchema());
    }
  }

  @Override
  public RecordMaterializer<Tuple> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    MessageType requestedSchema = readContext.getRequestedSchema();
    return new TupleRecordMaterializer(requestedSchema);
  }

}
