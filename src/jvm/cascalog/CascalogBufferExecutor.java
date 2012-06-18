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
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;

public class CascalogBufferExecutor extends BaseOperation implements Buffer {
    CascalogBuffer buf;

    public CascalogBufferExecutor(Fields outFields, CascalogBuffer buf) {
        super(outFields);
        this.buf = buf;
    }

    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        buf.prepare(flowProcess, operationCall);
    }

    public void operate(FlowProcess flowProcess, BufferCall bufCall) {
        buf.operate(flowProcess, bufCall);
    }

    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
        buf.cleanup(flowProcess, operationCall);
    }

}
