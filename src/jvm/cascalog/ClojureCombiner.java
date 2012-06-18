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

import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import java.util.List;

public class ClojureCombiner extends ClojureCombinerBase {

    public ClojureCombiner(Fields groupFields, List<Fields> argFields, Fields outFields,
        List<ParallelAgg> agg_specs) {
        super(groupFields, false, null, argFields, outFields, agg_specs, "cascalog.combiner.aggregator.size", 10000);
    }

    @Override
    protected void write(Tuple group, List<Object> val, OperationCall opCall) {
        TupleEntryCollector output = ((FunctionCall) opCall).getOutputCollector();
        Tuple t = new Tuple(group);
        for (Object o : val) {
            t.add(o);
        }
        output.add(t);
    }
}
