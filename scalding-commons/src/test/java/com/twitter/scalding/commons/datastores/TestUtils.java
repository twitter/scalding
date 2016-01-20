package com.twitter.scalding.commons.datastores;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class TestUtils {

    private static final String TMP_ROOT = "/tmp/unittests";

    public static String getTmpPath(FileSystem fs, String name) throws IOException {
        fs.mkdirs(new Path(TMP_ROOT));
        String full = TMP_ROOT + "/" + name;
        if (fs.exists(new Path(full))) {
            fs.delete(new Path(full), true);
        }
        return full;
    }
}
