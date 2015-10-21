package com.twitter.maple.tap;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;

public class StdoutTap extends Lfs {

    public StdoutTap() {
        super(new SequenceFile(Fields.ALL), getTempDir());
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
        System.out.println("");
        System.out.println("");
        System.out.println("RESULTS");
        System.out.println("-----------------------");
        while (it.hasNext()) {
            System.out.println(it.next().getTuple());
        }
        System.out.println("-----------------------");
        it.close();
        return true;
    }
}