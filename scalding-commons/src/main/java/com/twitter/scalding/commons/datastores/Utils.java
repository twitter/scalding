package com.twitter.scalding.commons.datastores;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Utils {

    public static FileSystem getFS(String path) throws IOException {
        return getFS(path, new Configuration());
    }

    public static FileSystem getFS(String path, Configuration conf) throws IOException {
        return new Path(path).getFileSystem(conf);
    }

    /**
     * Return true or false if the input is a long
     * @param input
     * @return boolean
     */
    public static boolean isLong(String input) {
        try {
            Long.parseLong(input);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
