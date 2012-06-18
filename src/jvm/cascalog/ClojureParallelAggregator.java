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
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import java.util.ArrayList;
import java.util.List;

public class ClojureParallelAggregator extends BaseOperation<Object> implements Aggregator<Object> {
    ParallelAgg agg;
    private int args;

    public ClojureParallelAggregator(Fields outfields, ParallelAgg agg, int args) {
        super(outfields);
        this.agg = agg;
        this.args = args;
    }

    public void prepare(FlowProcess flowProcess, OperationCall<Object> opCall) {
        this.agg.prepare(flowProcess, opCall);
    }

    public void start(FlowProcess flowProcess, AggregatorCall<Object> aggCall) {
        aggCall.setContext(null);
    }

    public void aggregate(FlowProcess flowProcess, AggregatorCall<Object> aggCall) {
        try {
            List<Object> initted;
            // workaround lack of empty tuples in cascading
            if (this.args > 0) {
                initted = agg.init(Util.tupleToList(aggCall.getArguments().getTuple()));
            } else {
                initted = agg.init(new ArrayList<Object>());
            }

            List<Object> currContext = (List<Object>) aggCall.getContext();
            if (currContext == null) {
                aggCall.setContext(initted);
            } else {
                aggCall.setContext(agg.combine(currContext, initted));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void complete(FlowProcess flowProcess, AggregatorCall<Object> aggCall) {
        try {
            aggCall.getOutputCollector().add(Util.coerceToTuple(aggCall.getContext()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
