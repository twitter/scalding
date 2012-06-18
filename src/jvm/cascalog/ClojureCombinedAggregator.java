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
import clojure.lang.ISeq;
import java.util.List;

public class ClojureCombinedAggregator extends BaseOperation<Object> implements Aggregator<Object> {
    private ParallelAgg _agg;

    public ClojureCombinedAggregator(Fields outfields, ParallelAgg agg) {
        super(outfields);
        _agg = agg;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Object> opCall) {
        _agg.prepare(flowProcess, opCall);
    }

    public void start(FlowProcess flowProcess, AggregatorCall<Object> aggCall) {
        aggCall.setContext(null);
    }

    public void aggregate(FlowProcess flowProcess, AggregatorCall<Object> aggCall) {
        try {
            List<Object> args = Util.tupleToList(aggCall.getArguments());
            List<Object> currContext = (List<Object>) aggCall.getContext();
            if (currContext == null) {
                aggCall.setContext(args);
            } else {
                aggCall.setContext(_agg.combine(currContext, args));
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
