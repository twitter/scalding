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
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import clojure.lang.RT;

public class ClojureBuffer extends ClojureCascadingBase implements Buffer {

    public ClojureBuffer(Fields out_fields, Object[] fn_spec, boolean stateful) {
        super(out_fields, fn_spec, stateful);
    }

    public void operate(FlowProcess flow_process, BufferCall buff_call) {
        ISeq result_seq = RT.seq(invokeFunction(IteratorSeq
            .create(new TupleSeqConverter(buff_call.getArgumentsIterator()))));
        TupleEntryCollector collector = buff_call.getOutputCollector();
        while (result_seq != null) {
            Object obj = result_seq.first();
            collector.add(Util.coerceToTuple(obj));
            result_seq = result_seq.next();
        }
    }
}
