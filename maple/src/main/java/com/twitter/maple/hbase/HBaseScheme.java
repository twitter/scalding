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
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.util.Util;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

/**
 * The HBaseScheme class is a {@link Scheme} subclass. It is used in conjunction with the {@link HBaseTap} to
 * allow for the reading and writing of data to and from a HBase cluster.
 *
 * @see HBaseTap
 */
public class HBaseScheme
    extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger(HBaseScheme.class);

  /** Field keyFields */
  private Fields keyField;
  /** String familyNames */
  private String[] familyNames;
  /** Field valueFields */
  private Fields[] valueFields;

  /** String columns */
  private transient String[] columns;
  /** Field fields */
  private transient byte[][] fields;


  /**
   * Constructor HBaseScheme creates a new HBaseScheme instance.
   *
   * @param keyFields   of type Fields
   * @param familyName  of type String
   * @param valueFields of type Fields
   */
  public HBaseScheme(Fields keyFields, String familyName, Fields valueFields) {
    this(keyFields, new String[]{familyName}, Fields.fields(valueFields));
  }

  /**
   * Constructor HBaseScheme creates a new HBaseScheme instance.
   *
   * @param keyFields   of type Fields
   * @param familyNames of type String[]
   * @param valueFields of type Fields[]
   */
  public HBaseScheme(Fields keyFields, String[] familyNames, Fields[] valueFields) {
    this.keyField = keyFields;
    this.familyNames = familyNames;
    this.valueFields = valueFields;

    setSourceSink(this.keyField, this.valueFields);

    validate();
  }

  /**
   * Constructor HBaseScheme creates a new HBaseScheme instance using fully qualified column names
   *
   * @param keyField    of type String
   * @param valueFields of type Fields
   */
  public HBaseScheme(Fields keyField, Fields valueFields) {
    this(keyField, Fields.fields(valueFields));
  }

  /**
   * Constructor HBaseScheme creates a new HBaseScheme instance using fully qualified column names
   *
   * @param keyField    of type Field
   * @param valueFields of type Field[]
   */
  public HBaseScheme(Fields keyField, Fields[] valueFields) {
    this.keyField = keyField;
    this.valueFields = valueFields;

    validate();

    setSourceSink(this.keyField, this.valueFields);
  }

  private void validate() {
    if (keyField.size() != 1) {
      throw new IllegalArgumentException("may only have one key field, found: " + keyField.print());
    }
  }

  private void setSourceSink(Fields keyFields, Fields[] columnFields) {
    Fields allFields = keyFields;

    if (columnFields.length != 0) {
      allFields = Fields.join(keyFields, Fields.join(columnFields)); // prepend
    }

    setSourceFields(allFields);
    setSinkFields(allFields);
  }

  /**
   * Method getFamilyNames returns the set of familyNames of this HBaseScheme object.
   *
   * @return the familyNames (type String[]) of this HBaseScheme object.
   */
  public String[] getFamilyNames() {
    HashSet<String> familyNameSet = new HashSet<String>();

    if (familyNames == null) {
      for (String columnName : columns(null, this.valueFields)) {
        int pos = columnName.indexOf(":");
        familyNameSet.add(hbaseColumn(pos > 0 ? columnName.substring(0, pos) : columnName));
      }
    } else {
      for (String familyName : familyNames) {
        familyNameSet.add(familyName);
      }
    }
    return familyNameSet.toArray(new String[0]);
  }

  @Override
  public void sourcePrepare(FlowProcess<? extends JobConf> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) {
    Object[] pair =
        new Object[]{sourceCall.getInput().createKey(), sourceCall.getInput().createValue()};

    sourceCall.setContext(pair);
  }

  @Override
  public void sourceCleanup(FlowProcess<? extends JobConf> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) {
    sourceCall.setContext(null);
  }

  @Override
  public boolean source(FlowProcess<? extends JobConf> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    Tuple result = new Tuple();

    Object key = sourceCall.getContext()[0];
    Object value = sourceCall.getContext()[1];
    boolean hasNext = sourceCall.getInput().next(key, value);
    if (!hasNext) { return false; }

    // Skip nulls
    if (key == null || value == null) { return true; }

    ImmutableBytesWritable keyWritable = (ImmutableBytesWritable) key;
    Result row = (Result) value;
    result.add(keyWritable);

    for (int i = 0; i < this.familyNames.length; i++) {
      String familyName = this.familyNames[i];
      byte[] familyNameBytes = Bytes.toBytes(familyName);
      Fields fields = this.valueFields[i];
      for (int k = 0; k < fields.size(); k++) {
        String fieldName = (String) fields.get(k);
        byte[] fieldNameBytes = Bytes.toBytes(fieldName);
        byte[] cellValue = row.getValue(familyNameBytes, fieldNameBytes);
        if (cellValue == null) {
            cellValue = new byte[0];
        }
        result.add(new ImmutableBytesWritable(cellValue));
      }
    }

    sourceCall.getIncomingEntry().setTuple(result);

    return true;
  }

  @Override
  public void sink(FlowProcess<? extends JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall)
      throws IOException {
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
    OutputCollector outputCollector = sinkCall.getOutput();
    Tuple key = tupleEntry.selectTuple(keyField);
    ImmutableBytesWritable keyBytes = (ImmutableBytesWritable) key.getObject(0);
    Put put = new Put(keyBytes.get());

    for (int i = 0; i < valueFields.length; i++) {
      Fields fieldSelector = valueFields[i];
      TupleEntry values = tupleEntry.selectEntry(fieldSelector);

      for (int j = 0; j < values.getFields().size(); j++) {
        Fields fields = values.getFields();
        Tuple tuple = values.getTuple();

        ImmutableBytesWritable valueBytes = (ImmutableBytesWritable) tuple.getObject(j);
        put.add(Bytes.toBytes(familyNames[i]), Bytes.toBytes((String) fields.get(j)), valueBytes.get());
      }
    }

    outputCollector.collect(null, put);
  }

  @Override
  public void sinkConfInit(FlowProcess<? extends JobConf> process,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    conf.setOutputFormat(TableOutputFormat.class);

    conf.setOutputKeyClass(ImmutableBytesWritable.class);
    conf.setOutputValueClass(Put.class);
  }

  @Override
  public void sourceConfInit(FlowProcess<? extends JobConf> process,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    conf.setInputFormat(TableInputFormat.class);

    String columns = getColumns();
    LOG.debug("sourcing from columns: {}", columns);
    conf.set(TableInputFormat.COLUMN_LIST, columns);
  }

  private String getColumns() {
    return Util.join(columns(this.familyNames, this.valueFields), " ");
  }

  private String[] columns(String[] familyNames, Fields[] fieldsArray) {
    if (columns != null) { return columns; }

    int size = 0;

    for (Fields fields : fieldsArray) { size += fields.size(); }

    columns = new String[size];

    int count = 0;

    for (int i = 0; i < fieldsArray.length; i++) {
      Fields fields = fieldsArray[i];

      for (int j = 0; j < fields.size(); j++) {
        if (familyNames == null) { columns[count++] = hbaseColumn((String) fields.get(j)); } else {
          columns[count++] = hbaseColumn(familyNames[i]) + (String) fields.get(j);
        }
      }
    }

    return columns;
  }

  private String hbaseColumn(String column) {
    if (column.indexOf(":") < 0) { return column + ":"; }

    return column;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) { return true; }
    if (object == null || getClass() != object.getClass()) { return false; }
    if (!super.equals(object)) { return false; }

    HBaseScheme that = (HBaseScheme) object;

    if (!Arrays.equals(familyNames, that.familyNames)) { return false; }
    if (keyField != null ? !keyField.equals(that.keyField) : that.keyField != null) {
      return false;
    }
    if (!Arrays.equals(valueFields, that.valueFields)) { return false; }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (keyField != null ? keyField.hashCode() : 0);
    result = 31 * result + (familyNames != null ? Arrays.hashCode(familyNames) : 0);
    result = 31 * result + (valueFields != null ? Arrays.hashCode(valueFields) : 0);
    return result;
  }
}
