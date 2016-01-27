package com.twitter.scalding.commons.datastores;

import junit.framework.Assert;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class VersionedStoreTest extends FSTestCase {

    @Test
    public void testCleanup() throws Exception {
        String tmp1 = TestUtils.getTmpPath(fs, "versions_test1");
        VersionedStore vs = new VersionedStore(tmp1);
        for (int i = 1; i <= 4; i ++) {
            String version = vs.createVersion(i);
            fs.mkdirs(new Path(version));
            vs.succeedVersion(i);
        }
        FileStatus[] files = fs.listStatus(new Path(tmp1));
        Assert.assertEquals(files.length, 8);
        vs.cleanup(2);
        files = fs.listStatus(new Path(tmp1));
        Assert.assertEquals(files.length, 4);
        for (FileStatus f : files) {
            String path = f.getPath().toString();
            Assert.assertTrue(path.endsWith("3") ||
            path.endsWith("4") ||
            path.endsWith("3.version") ||
            path.endsWith(("4.version")));
        }
    }

    // verify cleanup works correctly when datasets have success files only
    @Test
    public void testCleanupWithSuccessFiles() throws Exception {
        String tmp1 = TestUtils.getTmpPath(fs, "versions_test2");
        VersionedStore vs = new VersionedStore(tmp1);
        for (int i = 1; i <= 4; i ++) {
            String version = vs.createVersion(i);
            fs.mkdirs(new Path(version));
            fs.createNewFile(new Path(version, VersionedStore.HADOOP_SUCCESS_FLAG));
        }
        FileStatus[] files = fs.listStatus(new Path(tmp1));
        Assert.assertEquals(files.length, 4); // one success file per version
        vs.cleanup(2);
        files = fs.listStatus(new Path(tmp1));
        Assert.assertEquals(files.length, 2); // one success file per version after cleanup
        for (FileStatus f : files) {
            String path = f.getPath().toString();
            Assert.assertTrue(path.endsWith("3") ||
            path.endsWith("4"));
        }
    }

    // verify cleanup works correctly when datasets have both version suffix files and success files
    @Test
    public void testCleanupWithMix() throws Exception {
        String tmp1 = TestUtils.getTmpPath(fs, "versions_test3");
        VersionedStore vs = new VersionedStore(tmp1);
        for (int i = 1; i <= 4; i ++) {
            String version = vs.createVersion(i);
            fs.mkdirs(new Path(version));
            fs.createNewFile(new Path(version, VersionedStore.HADOOP_SUCCESS_FLAG));
            vs.succeedVersion(i); // adds .version file
        }
        FileStatus[] files = fs.listStatus(new Path(tmp1));
        Assert.assertEquals(files.length, 8); // one success file + version suffix per version
        vs.cleanup(2);
        files = fs.listStatus(new Path(tmp1));
        Assert.assertEquals(files.length, 4); // after cleanup
        for (FileStatus f : files) {
            String path = f.getPath().toString();
            Assert.assertTrue(path.endsWith("3") ||
            path.endsWith("4") ||
            path.endsWith("3.version") ||
            path.endsWith(("4.version")));
        }
    }

    @Test
    public void testMultipleVersions() throws Exception {
        String tmp1 = TestUtils.getTmpPath(fs, "versions_checker");
        VersionedStore vs = new VersionedStore(tmp1);
        for (int i = 1; i <= 4; i ++) {
            String version = vs.createVersion(i);
            fs.mkdirs(new Path(version));
            vs.succeedVersion(i);
        }
        new File(new Path(tmp1, "5" + VersionedStore.FINISHED_VERSION_SUFFIX).toString()).createNewFile();
        Path invalidPath = new Path(tmp1, "_test");
        fs.mkdirs(invalidPath);
        new File(new Path(invalidPath, VersionedStore.HADOOP_SUCCESS_FLAG).toString()).createNewFile();

        List<Long> allVersions = vs.getAllVersions();
        Set<Long> output = new HashSet<Long>();
        output.addAll(allVersions);
        Set<Long> expected = new HashSet<Long>();
        for (int i = 1; i <= 4; i ++) {
            expected.add(Long.valueOf(i));
        }

        Assert.assertEquals(output, expected);
    }

}

