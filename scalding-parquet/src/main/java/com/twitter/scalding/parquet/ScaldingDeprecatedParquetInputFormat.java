package com.twitter.scalding.parquet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.mapred.Container;

/**
 * This class is a clone of org.apache.parquet.hadoop.mapred.DeprecatedParquetInputFormat.
 * The purpose is patching a bug while we wait for this to land in upstream and for us to update
 * the version used. This class can be removed afterwards.
 *
 * The only code modification exists in the next() method, ensuring the value is set for the first
 * element.
 */
public class ScaldingDeprecatedParquetInputFormat<V> extends FileInputFormat<Void, Container<V>> {
  protected ParquetInputFormat<V> realInputFormat = new ParquetInputFormat();

  public ScaldingDeprecatedParquetInputFormat() {
  }

  public RecordReader<Void, Container<V>> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new RecordReaderWrapper(split, job, reporter);
  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    if (isTaskSideMetaData(job)) {
      return super.getSplits(job, numSplits);
    } else {
      List<Footer> footers = this.getFooters(job);
      List<ParquetInputSplit> splits = this.realInputFormat.getSplits(job, footers);
      if (splits == null) {
        return null;
      } else {
        InputSplit[] resultSplits = new InputSplit[splits.size()];
        int i = 0;

        ParquetInputSplit split;
        for(Iterator var7 = splits.iterator(); var7.hasNext(); resultSplits[i++] = new ParquetInputSplitWrapper(split)) {
          split = (ParquetInputSplit)var7.next();
        }

        return resultSplits;
      }
    }
  }

  public List<Footer> getFooters(JobConf job) throws IOException {
    return this.realInputFormat.getFooters(job, Arrays.asList(super.listStatus(job)));
  }

  public static boolean isTaskSideMetaData(JobConf job) {
    return job.getBoolean("parquet.task.side.metadata", Boolean.TRUE);
  }

  private static class ParquetInputSplitWrapper implements InputSplit {
    ParquetInputSplit realSplit;

    public ParquetInputSplitWrapper() {
    }

    public ParquetInputSplitWrapper(ParquetInputSplit realSplit) {
      this.realSplit = realSplit;
    }

    public long getLength() throws IOException {
      return this.realSplit.getLength();
    }

    public String[] getLocations() throws IOException {
      return this.realSplit.getLocations();
    }

    public void readFields(DataInput in) throws IOException {
      this.realSplit = new ParquetInputSplit();
      this.realSplit.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
      this.realSplit.write(out);
    }
  }

  private static class RecordReaderWrapper<V> implements RecordReader<Void, Container<V>> {
    private ParquetRecordReader<V> realReader;
    private long splitLen;
    private Container<V> valueContainer = null;
    private boolean firstRecord = false;
    private boolean eof = false;

    public RecordReaderWrapper(InputSplit oldSplit, JobConf oldJobConf, Reporter reporter) throws IOException {
      this.splitLen = oldSplit.getLength();

      try {
        this.realReader = new ParquetRecordReader(ParquetInputFormat.getReadSupportInstance(oldJobConf), ParquetInputFormat.getFilter(oldJobConf));
        if (oldSplit instanceof ParquetInputSplitWrapper) {
          this.realReader.initialize(((ParquetInputSplitWrapper)oldSplit).realSplit, oldJobConf, reporter);
        } else {
          if (!(oldSplit instanceof FileSplit)) {
            throw new IllegalArgumentException("Invalid split (not a FileSplit or ParquetInputSplitWrapper): " + oldSplit);
          }

          this.realReader.initialize((FileSplit)oldSplit, oldJobConf, reporter);
        }

        if (this.realReader.nextKeyValue()) {
          this.firstRecord = true;
          this.valueContainer = new Container();
          this.valueContainer.set(this.realReader.getCurrentValue());
        } else {
          this.eof = true;
        }

      } catch (InterruptedException var5) {
        Thread.interrupted();
        throw new IOException(var5);
      }
    }

    public void close() throws IOException {
      this.realReader.close();
    }

    public Void createKey() {
      return null;
    }

    public Container<V> createValue() {
      return this.valueContainer;
    }

    public long getPos() throws IOException {
      return (long)((float)this.splitLen * this.getProgress());
    }

    public float getProgress() throws IOException {
      try {
        return this.realReader.getProgress();
      } catch (InterruptedException var2) {
        Thread.interrupted();
        throw new IOException(var2);
      }
    }

    public boolean next(Void key, Container<V> value) throws IOException {
      if (this.eof) {
        return false;
      } else if (this.firstRecord) {
        this.firstRecord = false;
        // Only addition compared to DeprecatedParquetInputFormat
        if (value != null && value != this.valueContainer) {
          value.set(this.valueContainer.get());
        }
        // end of difference
        return true;
      } else {
        try {
          if (this.realReader.nextKeyValue()) {
            if (value != null) {
              value.set(this.realReader.getCurrentValue());
            }

            return true;
          }
        } catch (InterruptedException var4) {
          throw new IOException(var4);
        }

        this.eof = true;
        return false;
      }
    }
  }
}
