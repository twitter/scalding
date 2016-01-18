package com.twitter.scalding.commons.datastores;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class FSTestCase {
    public FileSystem local;
    public FileSystem fs;

    public FSTestCase() {
        try {
            local = FileSystem.getLocal(new Configuration());
            fs = FileSystem.get(new Configuration());
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }
}
