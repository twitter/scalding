package com.twitter.scalding.commons.datastores;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class VersionedStore {
    public static final String FINISHED_VERSION_SUFFIX = ".version";
    public static final String HADOOP_SUCCESS_FLAG = "_SUCCESS";

    private String root;
    private FileSystem fs;

    public VersionedStore(String path) throws IOException {
      this(Utils.getFS(path), path);
    }

    public VersionedStore(FileSystem fs, String path) throws IOException {
      this.fs = fs;
      root = path;
      mkdirs(root);
    }

    public VersionedStore(Path path, Configuration conf) throws IOException {
        this.fs = path.getFileSystem(conf);
        this.root = path.toString();
    }

    public FileSystem getFileSystem() {
        return fs;
    }

    public String getRoot() {
        return root;
    }

    public String versionPath(long version) {
        return new Path(getRoot(), "" + version).toString();
    }

    public String mostRecentVersionPath() throws IOException {
        Long v = mostRecentVersion();
        return (v == null) ? null : versionPath(v);
    }

    public String mostRecentVersionPath(long maxVersion) throws IOException {
        Long v = mostRecentVersion(maxVersion);
        return (v == null) ? null : versionPath(v);
    }

    public Long mostRecentVersion() throws IOException {
        return mostRecentVersion(false, null);
    }

    public Long mostRecentVersion(boolean skipVersionSuffix) throws IOException {
        return mostRecentVersion(skipVersionSuffix, null);
    }

    public Long mostRecentVersion(long maxVersion) throws IOException {
        return mostRecentVersion(false, maxVersion);
    }

    public Long mostRecentVersion(boolean skipVersionSuffix, Long maxVersion) throws IOException {
        List<Long> all = getAllVersions(skipVersionSuffix);
        if (maxVersion == null) {
            return (all.size() == 0) ? null : all.get(0);
        } else {
            for(Long v: all) {
                if(v <= maxVersion)
                    return v;
            }
            return null;
        }
    }

    public long newVersion() {
        return System.currentTimeMillis();
    }

    public String createVersion() throws IOException {
        return createVersion(newVersion());
    }

    public String createVersion(long version) throws IOException {
        String ret = versionPath(version);
        if(getAllVersions().contains(version))
            throw new RuntimeException("Version already exists or data already exists");
        else {
            //in case there's an incomplete version there, delete it
            fs.delete(new Path(versionPath(version)), true);
            return ret;
        }
    }

    public void failVersion(String path) throws IOException {
        deleteVersion(validateAndGetVersion(path));
    }

    public void deleteVersion(long version) throws IOException {
        // Be sure to delete success indicators before data
        fs.delete(new Path(tokenPath(version)), false);
        fs.delete(new Path(successFlagPath(version)), false);
        fs.delete(new Path(versionPath(version)), true);
    }

    public void succeedVersion(String path) throws IOException {
        succeedVersion(validateAndGetVersion(path));
    }

    public void succeedVersion(long version) throws IOException {
        createNewFile(tokenPath(version));
    }

    public void cleanup() throws IOException {
        // Default behavior is to clean up NOTHING
        cleanup(-1);
    }

    public void cleanup(int versionsToKeep) throws IOException {
        if (versionsToKeep < 0) return;
        final List<Long> versions = getAllVersions();
        int numExisting = versions.size(); 
        if (numExisting <= versionsToKeep) return;
        for (Long v : versions.subList(versionsToKeep, numExisting)) {
            deleteVersion(v);
        }
    }

    /**
     * Sorted from most recent to oldest
     */
    public List<Long> getAllVersions() throws IOException {
        return getAllVersions(false);
    }

    public List<Long> getAllVersions(boolean skipVersionSuffix) throws IOException {

        Path rootPath = new Path(getRoot());
        if (getFileSystem().exists(rootPath)) {
            // we use a set so we can automatically de-dupe folders that
            // have both version suffix and success flag below
            Set<Long> ret = new HashSet<Long>();
            for(Path p: listDir(getRoot())) {
                final FileStatus status = getFileSystem().getFileStatus(p);
                if (skipVersionSuffix) {
                    // backwards compatible if version suffix does not exist
                    if(Utils.isLong(p.getName())) {
                        ret.add(Long.valueOf(p.getName()));
                    }
                } else {
                    if (!p.getName().startsWith("_")) {
                        try {
                            if (p.getName().endsWith(FINISHED_VERSION_SUFFIX)) {
                                ret.add(validateAndGetVersion(p.toString()));
                            } else if (status != null && status.isDir() && getFileSystem().exists(new Path(p, HADOOP_SUCCESS_FLAG))) {
                                // FORCE the _SUCCESS flag into the versioned store directory.
                                ret.add(validateAndGetVersion(p.toString() + FINISHED_VERSION_SUFFIX));
                            }
                        } catch (RuntimeException e) {
                            // Skip this version
                            continue;
                        }
                    }
                }
            }
            List<Long> retList = new ArrayList<Long>(ret);
            // now sort the versions most recent first per the api contract
            Collections.sort(retList);
            Collections.reverse(retList);
            return retList;
        } else {
            return Collections.emptyList();
        }
    }

    public boolean hasVersion(long version) throws IOException {
        return getAllVersions().contains(version);
    }

    private String tokenPath(long version) {
        return new Path(root, "" + version + FINISHED_VERSION_SUFFIX).toString();
    }

    /** The path to the hadoop-created success flag file which may or may not exist */
    private String successFlagPath(long version) {
        return new Path(versionPath(version), HADOOP_SUCCESS_FLAG).toString();
    }

    private Path normalizePath(String p) {
        return new Path(p).makeQualified(fs);
    }

    private long validateAndGetVersion(String path) {
        Path parent = new Path(path).getParent();
        if(!normalizePath(path).getParent().equals(normalizePath(root))) {
            throw new RuntimeException(path + " " + parent + " is not part of the versioned store located at " + root);
        }
        Long v = parseVersion(path);
        if (v==null) throw new RuntimeException(path + " is not a valid version");

        // Check that versioned folder exists
        Path versionPath = new Path(parent, v.toString());
        try {
            FileStatus status = getFileSystem().getFileStatus(versionPath);
            if (status == null || !status.isDir()) throw new RuntimeException(versionPath + " is not a valid version subfolder");
        } catch (IOException e) {
            throw new RuntimeException("could not stat path: " + versionPath);
        }

        return v;
    }

    public Long parseVersion(String path) {
        String name = new Path(path).getName();
        if(name.endsWith(FINISHED_VERSION_SUFFIX)) {
            name = name.substring(0, name.length()-FINISHED_VERSION_SUFFIX.length());
        }
        try {
            return Long.parseLong(name);
        } catch(NumberFormatException e) {
            return null;
        }
    }

    private void createNewFile(String path) throws IOException {
        if(fs instanceof LocalFileSystem)
            new File(path).createNewFile();
        else
            fs.createNewFile(new Path(path));
    }

    private void mkdirs(String path) throws IOException {
        if(fs instanceof LocalFileSystem)
            new File(path).mkdirs();
        else {
            try {
                fs.mkdirs(new Path(path));
            } catch (AccessControlException e) {
                throw new RuntimeException("Root directory doesn't exist, and user doesn't have the permissions " +
                                           "to create" + path + ".", e);
            }
        }
    }

    private List<Path> listDir(String dir) throws IOException {
        List<Path> ret = new ArrayList<Path>();
        if(fs instanceof LocalFileSystem) {
            for(File f: new File(dir).listFiles()) {
                ret.add(new Path(f.getAbsolutePath()));
            }
        } else {
            for(FileStatus status: fs.listStatus(new Path(dir))) {
                ret.add(status.getPath());
            }
        }
        return ret;
    }
}
