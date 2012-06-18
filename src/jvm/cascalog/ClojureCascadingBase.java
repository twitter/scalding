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
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import clojure.lang.IFn;
import clojure.lang.ISeq;

public class ClojureCascadingBase extends BaseOperation {
    private byte[] serialized_spec;
    private Object[] fn_spec;

    private boolean stateful;
    private Object state;
    private IFn fn;

    public void initialize(Object[] fn_spec, boolean stateful) {
        serialized_spec = KryoService.serialize(fn_spec);
        this.stateful = stateful;
    }

    public ClojureCascadingBase(Object[] fn_spec, boolean stateful) {
        initialize(fn_spec, stateful);
    }

    public ClojureCascadingBase(Fields fields, Object[] fn_spec, boolean stateful) {
        super(fields);
        initialize(fn_spec, stateful);
    }

    @Override
    public void prepare(FlowProcess flow_process, OperationCall op_call) {
        this.fn_spec = (Object[]) KryoService.deserialize(serialized_spec);
        this.fn = Util.bootFn(fn_spec);
        if (stateful) {
            try {
                state = this.fn.invoke();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected Object applyFunction(ISeq seq) {
        try {
            if (stateful) {
                return this.fn.applyTo(seq.cons(state));
            } else {
                return this.fn.applyTo(seq);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Object invokeFunction(Object arg) {
        try {
            if (stateful) {
                return this.fn.invoke(state, arg);
            } else {
                return this.fn.invoke(arg);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    protected Object invokeFunction() {
        try {
            if (stateful) {
                return this.fn.invoke(state);
            } else {
                return this.fn.invoke();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall op_call) {
        if (stateful) {
            try {
                this.fn.invoke(state);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
