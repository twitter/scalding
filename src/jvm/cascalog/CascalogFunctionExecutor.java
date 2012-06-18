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
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;

public class CascalogFunctionExecutor extends BaseOperation implements Function {
    CascalogFunction fn;

    public CascalogFunctionExecutor(Fields out_fields, CascalogFunction fn) {
        super(out_fields);
        this.fn = fn;
    }

    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        fn.prepare(flowProcess, operationCall);
    }

    public void operate(FlowProcess flow_process, FunctionCall fn_call) {
        fn.operate(flow_process, fn_call);
    }

    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
        fn.cleanup(flowProcess, operationCall);
    }

}
