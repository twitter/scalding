package com.twitter.maple.tap;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class MemorySinkTap extends Lfs {
    private List<Tuple> results;
    private Fields fields;

    public MemorySinkTap(List<Tuple> tuples, Fields fields) {
        super(new SequenceFile(Fields.ALL), getTempDir());
        this.results = tuples;
        this.fields = fields;
    }

    public MemorySinkTap(List<Tuple> tuples) {
        this(tuples, new Fields());
    }

    public static String getTempDir() {
        final File temp;
        try {
            temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        temp.deleteOnExit();
        if (!(temp.delete())) {
            throw new RuntimeException("Could not delete temp file: " + temp.getAbsolutePath());
        }

        return temp.getAbsoluteFile().getPath();
    }

    @Override
    public boolean commitResource(Configuration conf) throws java.io.IOException {
        TupleEntryIterator it = new HadoopFlowProcess(conf).openTapForRead(this);

        boolean first_time = true;
        while (it.hasNext()) {
            TupleEntry tuple = it.next();
            results.add(tuple.getTupleCopy());

            if (first_time) {
                fields = tuple.getFields();
                first_time = false;
            }
        }
        it.close();
        return true;
    }
}
