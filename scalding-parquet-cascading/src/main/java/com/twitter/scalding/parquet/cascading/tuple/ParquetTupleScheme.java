package com.twitter.scalding.parquet.cascading.tuple;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.mapred.Container;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.parquet.schema.MessageType;

import static org.apache.parquet.Preconditions.checkNotNull;

/**
  * A Cascading Scheme that converts Parquet groups into Cascading tuples.
  * If you provide it with sourceFields, it will selectively materialize only the columns for those fields.
  * The names must match the names in the Parquet schema.
  * If you do not provide sourceFields, or use Fields.ALL or Fields.UNKNOWN, it will create one from the
  * Parquet schema.
  * Currently, only primitive types are supported. TODO: allow nested fields in the Parquet schema to be
  * flattened to a top-level field in the Cascading tuple.
  *
  * @author Avi Bryant
  */

public class ParquetTupleScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]>{

  private static final long serialVersionUID = 0L;
  private String parquetSchema;
  private final FilterPredicate filterPredicate;

  public ParquetTupleScheme() {
    super();
    this.filterPredicate = null;
  }

  public ParquetTupleScheme(Fields sourceFields) {
    super(sourceFields);
    this.filterPredicate = null;
  }

  public ParquetTupleScheme(FilterPredicate filterPredicate) {
    this.filterPredicate = checkNotNull(filterPredicate, "filterPredicate");
  }

  public ParquetTupleScheme(FilterPredicate filterPredicate, Fields sourceFields) {
    super(sourceFields);
    this.filterPredicate = checkNotNull(filterPredicate, "filterPredicate");
  }

  /**
   * ParquetTupleScheme constructor used a sink need to be implemented
   *
   * @param sourceFields used for the reading step
   * @param sinkFields used for the writing step
   * @param schema is mandatory if you add sinkFields and needs to be the
   * toString() from a MessageType. This value is going to be parsed when the
   * parquet file will be created.
   */
  public ParquetTupleScheme(Fields sourceFields, Fields sinkFields, final String schema) {
    super(sourceFields, sinkFields);
    parquetSchema = schema;
    this.filterPredicate = null;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void sourceConfInit(FlowProcess<JobConf> fp,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {

    if (filterPredicate != null) {
      ParquetInputFormat.setFilterPredicate(jobConf, filterPredicate);
    }

    jobConf.setInputFormat(DeprecatedParquetInputFormat.class);
    ParquetInputFormat.setReadSupportClass(jobConf, TupleReadSupport.class);
    TupleReadSupport.setRequestedFields(jobConf, getSourceFields());
 }

 @Override
 public Fields retrieveSourceFields(FlowProcess<JobConf> flowProcess, Tap tap) {
    MessageType schema = readSchema(flowProcess, tap);
    SchemaIntersection intersection = new SchemaIntersection(schema, getSourceFields());

    setSourceFields(intersection.getSourceFields());

    return getSourceFields();
  }

  private MessageType readSchema(FlowProcess<JobConf> flowProcess, Tap tap) {
    try {
      Hfs hfs;

      if( tap instanceof CompositeTap )
        hfs = (Hfs) ( (CompositeTap) tap ).getChildTaps().next();
      else
        hfs = (Hfs) tap;

      List<Footer> footers = getFooters(flowProcess, hfs);

      if(footers.isEmpty()) {
        throw new TapException("Could not read Parquet metadata at " + hfs.getPath());
      } else {
        return footers.get(0).getParquetMetadata().getFileMetaData().getSchema();
      }
    } catch (IOException e) {
      throw new TapException(e);
    }
  }

   private List<Footer> getFooters(FlowProcess<JobConf> flowProcess, Hfs hfs) throws IOException {
     JobConf jobConf = flowProcess.getConfigCopy();
     DeprecatedParquetInputFormat format = new DeprecatedParquetInputFormat();
     format.addInputPath(jobConf, hfs.getPath());
     return format.getFooters(jobConf);
   }

  @SuppressWarnings("unchecked")
  @Override
  public boolean source(FlowProcess<JobConf> fp, SourceCall<Object[], RecordReader> sc)
      throws IOException {
    Container<Tuple> value = (Container<Tuple>) sc.getInput().createValue();
    boolean hasNext = sc.getInput().next(null, value);
    if (!hasNext) { return false; }

    // Skip nulls
    if (value == null) { return true; }

    sc.getIncomingEntry().setTuple(value.get());
    return true;
  }


  @SuppressWarnings("rawtypes")
  @Override
  public void sinkConfInit(FlowProcess<JobConf> fp,
          Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
    DeprecatedParquetOutputFormat.setAsOutputFormat(jobConf);
    jobConf.set(TupleWriteSupport.PARQUET_CASCADING_SCHEMA, parquetSchema);
    ParquetOutputFormat.setWriteSupportClass(jobConf, TupleWriteSupport.class);
  }

  @Override
  public boolean isSink() {
    return parquetSchema != null;
  }

  @Override
  public void sink(FlowProcess<JobConf> fp, SinkCall<Object[], OutputCollector> sink)
          throws IOException {
    TupleEntry tuple = sink.getOutgoingEntry();
    OutputCollector outputCollector = sink.getOutput();
    outputCollector.collect(null, tuple);
  }
}
