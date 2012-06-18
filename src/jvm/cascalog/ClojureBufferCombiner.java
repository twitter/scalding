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
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;

import java.util.Arrays;
import java.util.List;

public class ClojureBufferCombiner extends ClojureCombinerBase {

    private CombinerSpec spec;

    public ClojureBufferCombiner(Fields groupFields, Fields sortFields, Fields args,
        Fields outFields, CombinerSpec spec) {
        super(groupFields, true, sortFields, Arrays.asList(args), outFields,
                Arrays.asList((ParallelAgg) new ClojureParallelAgg(spec)),
                "cascalog.combiner.buffer.size", 200);
        this.spec = spec;
    }

    private IFn extract_fn = null;

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        super.prepare(flowProcess, operationCall);
        extract_fn = Util.bootFn(spec.extractor_spec);
    }

    @Override
    protected void write(Tuple group, List<Object> vals, OperationCall opCall) {
        TupleEntryCollector output = ((FunctionCall) opCall).getOutputCollector();

        if (vals.size() != 1) {
            throw new RuntimeException(
                "Should only have one object in buffer combiner before extraction " + vals.size()
                + ":" + vals.toString());
        }
        Object val = vals.get(0);
        try {
            ISeq result_seq = RT.seq(extract_fn.invoke(val));
            while (result_seq != null) {
                Tuple t = Util.coerceToTuple(result_seq.first());
                Tuple emit = new Tuple(group);
                emit.addAll(t);
                output.add(emit);
                result_seq = result_seq.next();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
