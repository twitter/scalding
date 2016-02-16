package com.twitter.scalding.parquet.cascading.tuple;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import cascading.tuple.Fields;

import java.util.List;
import java.util.ArrayList;

public class SchemaIntersection {

  private final MessageType requestedSchema;
  private final Fields sourceFields;

  public SchemaIntersection(MessageType fileSchema, Fields requestedFields) {
    if(requestedFields == Fields.UNKNOWN)
      requestedFields = Fields.ALL;

    Fields newFields = Fields.NONE;
    List<Type> newSchemaFields = new ArrayList<Type>();
    int schemaSize = fileSchema.getFieldCount();

    for (int i = 0; i < schemaSize; i++) {
      Type type = fileSchema.getType(i);
      Fields name = new Fields(type.getName());

      if(requestedFields.contains(name)) {
        newFields = newFields.append(name);
        newSchemaFields.add(type);
      }
    }

    this.sourceFields = newFields;
    this.requestedSchema = new MessageType(fileSchema.getName(), newSchemaFields);
  }

  public MessageType getRequestedSchema() {
    return requestedSchema;
  }

  public Fields getSourceFields() {
    return sourceFields;
  }
}
