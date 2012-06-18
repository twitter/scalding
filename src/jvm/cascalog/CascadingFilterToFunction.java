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
import cascading.operation.*;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class CascadingFilterToFunction extends BaseOperation implements Function {
    Filter filter;

    public CascadingFilterToFunction(String outfield, Filter filter) {
        super(new Fields(outfield));
        this.filter = filter;
    }

    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        filter.prepare(flowProcess, operationCall);
    }

    public void operate(FlowProcess process, FunctionCall call) {
        boolean ret = !filter.isRemove(process, new FilterFunctionCall(call));
        call.getOutputCollector().add(new Tuple(ret));
    }

    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
        filter.cleanup(flowProcess, operationCall);
    }

}
