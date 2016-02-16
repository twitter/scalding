package com.twitter.scalding.parquet.cascading.tuple;

import cascading.tuple.TupleEntry;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 */
public class TupleWriteSupport extends WriteSupport<TupleEntry> {

  private RecordConsumer recordConsumer;
  private MessageType rootSchema;
  public static final String PARQUET_CASCADING_SCHEMA = "parquet.cascading.schema";

  //@Override
  public String getName() {
    return "cascading";
  }

  @Override
  public WriteContext init(Configuration configuration) {
    String schema = configuration.get(PARQUET_CASCADING_SCHEMA);
    rootSchema = MessageTypeParser.parseMessageType(schema);
    return new WriteContext(rootSchema, new HashMap<String, String>());
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(TupleEntry record) {
    recordConsumer.startMessage();
    final List<Type> fields = rootSchema.getFields();

    for (int i = 0; i < fields.size(); i++) {
      Type field = fields.get(i);

      if (record == null || record.getObject(field.getName()) == null) {
        continue;
      }
      recordConsumer.startField(field.getName(), i);
      if (field.isPrimitive()) {
        writePrimitive(record, field.asPrimitiveType());
      } else {
        throw new UnsupportedOperationException("Complex type not implemented");
      }
      recordConsumer.endField(field.getName(), i);
    }
    recordConsumer.endMessage();
  }

  private void writePrimitive(TupleEntry record, PrimitiveType field) {
    switch (field.getPrimitiveTypeName()) {
      case BINARY:
        recordConsumer.addBinary(Binary.fromString(record.getString(field.getName())));
        break;
      case BOOLEAN:
        recordConsumer.addBoolean(record.getBoolean(field.getName()));
        break;
      case INT32:
        recordConsumer.addInteger(record.getInteger(field.getName()));
        break;
      case INT64:
        recordConsumer.addLong(record.getLong(field.getName()));
        break;
      case DOUBLE:
        recordConsumer.addDouble(record.getDouble(field.getName()));
        break;
      case FLOAT:
        recordConsumer.addFloat(record.getFloat(field.getName()));
        break;
      case FIXED_LEN_BYTE_ARRAY:
        throw new UnsupportedOperationException("Fixed len byte array type not implemented");
      case INT96:
        throw new UnsupportedOperationException("Int96 type not implemented");
      default:
        throw new UnsupportedOperationException(field.getName() + " type not implemented");
    }
  }
}
