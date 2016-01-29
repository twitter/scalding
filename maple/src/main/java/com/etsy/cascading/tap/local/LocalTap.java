package com.etsy.cascading.tap.local;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Util;

/**
 * A shunt around the restrictions on taps that can run under the
 * LocalFlowProcess. This class delegates to Lfs for all work, yet can run in
 * Cascading local mode.
 *
 * Due to what appears to be a bug in LocalFlowStep#initTaps, schemes are not
 * allowed to side-effect the properties passed to sourceConfInit and
 * sinkConfInit. This appears to be a non-issue because local schemes never set
 * any properties. overwriteProperties can likely be removed, but is kept in
 * case custom local schemes eventually set properties (in a future release of
 * Cascading where that's possible).
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class LocalTap<SourceCtx, SinkCtx> extends Tap<Properties, RecordReader, OutputCollector> {
    private static final long serialVersionUID = 3480480638297770870L;

    private static Logger LOG = Logger.getLogger(LocalTap.class.getName());

    private String path;
    private JobConf defaults;
    private Lfs lfs;

    public LocalTap(String path, Scheme<Configuration, RecordReader, OutputCollector, SourceCtx, SinkCtx> scheme,
            SinkMode sinkMode) {
        super(new LocalScheme<SourceCtx, SinkCtx>(scheme), sinkMode);
        setup(path, scheme);
    }

    public LocalTap(String path, Scheme<Configuration, RecordReader, OutputCollector, SourceCtx, SinkCtx> scheme) {
        super(new LocalScheme<SourceCtx, SinkCtx>(scheme));
        setup(path, scheme);
    }

    private void setup(String path, Scheme<Configuration, RecordReader, OutputCollector, SourceCtx, SinkCtx> scheme) {
        this.path = path;

        /*
         * LocalTap requires your system Hadoop configuration for defaults to
         * supply the wrapped Lfs. Make sure you have your serializations and
         * serialization tokens defined there.
         */
        defaults = new JobConf();

        // HACK: c.t.h.TextLine checks this property for .zip files; the check
        // assumes the list is non-empty, which we mock up, here
        defaults.set("mapred.input.dir", path);

        // HACK: Parquet uses this property to generate unique file names
        defaults.set("mapred.task.partition", "0");

        // HACK: disable Parquet counters
        defaults.set("parquet.benchmark.bytes.read", "false");
        defaults.set("parquet.benchmark.bytes.total", "false");
        defaults.set("parquet.benchmark.time.read", "false");

        ((LocalScheme<SourceCtx, SinkCtx>) this.getScheme()).setDefaults(defaults);

        lfs = new Lfs(scheme, path);
        ((LocalScheme<SourceCtx, SinkCtx>) this.getScheme()).setLfs(lfs);
    }

    @Override
    public String getIdentifier() {
        return path;
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<? extends Properties> flowProcess, RecordReader input) throws IOException {
        JobConf jobConf = mergeDefaults("LocalTap#openForRead", flowProcess.getConfigCopy(), defaults);
        return lfs.openForRead(new HadoopFlowProcess(jobConf));
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<? extends Properties> flowProcess, OutputCollector output)
            throws IOException {
        JobConf jobConf = mergeDefaults("LocalTap#openForWrite", flowProcess.getConfigCopy(), defaults);
        return lfs.openForWrite(new HadoopFlowProcess(jobConf));
    }

    @Override
    public boolean createResource(Properties conf) throws IOException {
        return lfs.createResource(mergeDefaults("LocalTap#createResource", conf, defaults));
    }

    @Override
    public boolean deleteResource(Properties conf) throws IOException {
        return lfs.deleteResource(mergeDefaults("LocalTap#deleteResource", conf, defaults));
    }

    @Override
    public boolean resourceExists(Properties conf) throws IOException {
        return lfs.resourceExists(mergeDefaults("LocalTap#resourceExists", conf, defaults));
    }

    @Override
    public long getModifiedTime(Properties conf) throws IOException {
        return lfs.getModifiedTime(mergeDefaults("LocalTap#getModifiedTime", conf, defaults));
    }

    private static JobConf mergeDefaults(String methodName, Properties properties, JobConf defaults) {
        LOG.fine(methodName + " is merging defaults with: " + properties);
        return HadoopUtil.createJobConf(properties, defaults);
    }

    private static Properties overwriteProperties(Properties properties, JobConf jobConf) {
        for (Map.Entry<String, String> entry : jobConf) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    /**
     * Scheme used only for fields and configuration.
     */
    private static class LocalScheme<SourceContext, SinkContext> extends
            Scheme<Properties, RecordReader, OutputCollector, SourceContext, SinkContext> {
        private static final long serialVersionUID = 5710119342340369543L;

        private Scheme<Configuration, RecordReader, OutputCollector, SourceContext, SinkContext> scheme;
        private JobConf defaults;
        private Lfs lfs;

        public LocalScheme(Scheme<Configuration, RecordReader, OutputCollector, SourceContext, SinkContext> scheme) {
            super(scheme.getSourceFields(), scheme.getSinkFields());
            this.scheme = scheme;
        }

        private void setDefaults(JobConf defaults) {
            this.defaults = defaults;
        }

        private void setLfs(Lfs lfs) {
            this.lfs = lfs;
        }

        @Override
        public Fields retrieveSourceFields(FlowProcess<? extends Properties> flowProcess,
                Tap tap) {
            return scheme.retrieveSourceFields(new HadoopFlowProcess(defaults), lfs);
        }

        @Override
        public void presentSourceFields(FlowProcess<? extends Properties> flowProcess,
                Tap tap, Fields fields) {
            scheme.presentSourceFields(new HadoopFlowProcess(defaults), lfs, fields);
        }

        @Override
        public void sourceConfInit(FlowProcess<? extends Properties> flowProcess,
                Tap<Properties, RecordReader, OutputCollector> tap, Properties conf) {
            JobConf jobConf = mergeDefaults("LocalScheme#sourceConfInit", conf, defaults);
            scheme.sourceConfInit(new HadoopFlowProcess(jobConf), lfs, jobConf);
            overwriteProperties(conf, jobConf);
        }

        @Override
        public Fields retrieveSinkFields(FlowProcess<? extends Properties> flowProcess,
                Tap tap) {
            return scheme.retrieveSinkFields(new HadoopFlowProcess(defaults), lfs);
        }

        @Override
        public void presentSinkFields(FlowProcess<? extends Properties> flowProcess,
                Tap tap, Fields fields) {
            scheme.presentSinkFields(new HadoopFlowProcess(defaults), lfs, fields);
        }
            
        @Override
        public void sinkConfInit(FlowProcess<? extends Properties> flowProcess,
                Tap<Properties, RecordReader, OutputCollector> tap, Properties conf) {
            JobConf jobConf = mergeDefaults("LocalScheme#sinkConfInit", conf, defaults);
            scheme.sinkConfInit(new HadoopFlowProcess(jobConf), lfs, jobConf);
            overwriteProperties(conf, jobConf);
        }

        @Override
        public boolean source(FlowProcess<? extends Properties> flowProcess, SourceCall<SourceContext, RecordReader> sourceCall)
                throws IOException {
            throw new RuntimeException("LocalTap#source is never called");
        }

        @Override
        public void sink(FlowProcess<? extends Properties> flowProcess, SinkCall<SinkContext, OutputCollector> sinkCall)
                throws IOException {
            throw new RuntimeException("LocalTap#sink is never called");
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object)
            return true;
        if (!(object instanceof LocalTap))
            return false;
        if (!super.equals(object))
            return false;

        LocalTap localTap = (LocalTap) object;

        if (path != null ? !path.equals(localTap.path) : localTap.path != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (path != null ? path.hashCode() : 0);
        return result;
    }

    /** @see Object#toString() */
    @Override
    public String toString() {
        if (path != null)
            return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[\"" + Util.sanitizeUrl(path) + "\"]";
        else
            return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[not initialized]";
    }
}
