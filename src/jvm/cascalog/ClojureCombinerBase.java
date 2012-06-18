/*
    Copyright 2010 Nathan Marz
 
    Project and contact information: http://www.cascalog.org/ 

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
   
        http://www.apache.org/licenses/LICENSE-2.0
   
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package cascalog;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;
import org.apache.hadoop.mapred.JobConf;

import java.util.*;
import java.util.Map.Entry;

public abstract class ClojureCombinerBase extends BaseOperation implements Function {
    private List<ParallelAgg> aggs;
    private Fields groupFields;
    private Fields sortFields;
    private List<Fields> argFields;
    private boolean includeSort;
    private String cacheConfArg;
    private int defaultCacheSize;
    private int cacheSize;

    private static Fields appendFields(Fields start, Fields... rest) {
        for (Fields f : rest) {
            if (f != null) {
                start = start.append(f);
            }
        }
        return start;
    }

    public ClojureCombinerBase(Fields groupFields, boolean includeSort, Fields sortFields,
        List<Fields> argFields, Fields outFields, List<ParallelAgg> agg_specs, String cacheConfArg,
        int defaultCacheSize) {
        super(appendFields(groupFields, sortFields, outFields));
        if (argFields.size() != agg_specs.size()) {
            throw new IllegalArgumentException("All lists to ClojureCombiner must be same length");
        }
        this.aggs = new ArrayList<ParallelAgg>(agg_specs);
        this.groupFields = groupFields;
        this.sortFields = sortFields;
        this.includeSort = includeSort;
        this.argFields = new ArrayList<Fields>(argFields);
        this.cacheConfArg = cacheConfArg;
        this.defaultCacheSize = defaultCacheSize;
    }

    //map from grouping to combiner to combined tuple
    LinkedHashMap<Tuple, Map<Integer, List<Object>>> combined;

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        JobConf jc = ((HadoopFlowProcess) flowProcess).getJobConf();
        this.cacheSize = jc.getInt(this.cacheConfArg, this.defaultCacheSize);
        combined = new LinkedHashMap<Tuple, Map<Integer, List<Object>>>(1000, (float) 0.75, true);
        for(ParallelAgg agg: aggs) {
            agg.prepare(flowProcess, operationCall);
        }
    }

    public void operate(FlowProcess fp, FunctionCall fc) {
        Tuple group = fc.getArguments().selectTuple(groupFields);
        Tuple sortArgs = null;
        if (includeSort) {
            if (sortFields != null) {
                sortArgs = fc.getArguments().selectTuple(sortFields);
            } else {
                sortArgs = new Tuple();
            }
        }
        Map<Integer, List<Object>> vals = combined.get(group);
        if (vals == null) {
            vals = new HashMap<Integer, List<Object>>(aggs.size());
            combined.put(group, vals);
        }
        for (int i = 0; i < aggs.size(); i++) {
            try {
                Fields specArgFields = argFields.get(i);
                List<Object> val;
                ParallelAgg agg = aggs.get(i);
                if (specArgFields == null) {
                    val = agg.init(new ArrayList<Object>());
                } else {
                    Tuple args = fc.getArguments().selectTuple(specArgFields);
                    List<Object> toApply = new ArrayList<Object>();
                    if(sortArgs!=null) {
                        toApply.add(Util.tupleToList(sortArgs));
                    }
                    Util.tupleIntoList(toApply, args);                    
                    val = agg.init(toApply);
                }

                if (vals.get(i) != null) {
                    List<Object> existing = vals.get(i);
                    val = agg.combine(existing, val);
                }
                vals.put(i, val);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        combined.put(group, vals);

        if (combined.size() >= this.cacheSize) {
            Tuple evict = combined.keySet().iterator().next();
            Map<Integer, List<Object>> removing = combined.remove(evict);
            writeMap(evict, removing, fc);
        }
    }

    private void writeMap(Tuple group, Map<Integer, List<Object>> val, OperationCall opCall) {
        List<Object> toWrite = new ArrayList<Object>(val.get(0));
        for (int i = 1; i < aggs.size(); i++) {
            toWrite.addAll(val.get(i));
        }
        write(group, toWrite, opCall);
    }

    protected abstract void write(Tuple group, List<Object> val, OperationCall opCall);

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
        for (Entry<Tuple, Map<Integer, List<Object>>> e : combined.entrySet()) {
            writeMap(e.getKey(), e.getValue(), operationCall);
        }
    }
}
