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
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import clojure.lang.ISeq;

public class ClojureFilter extends ClojureCascadingBase implements Filter {
    public ClojureFilter(Object[] fn_spec, boolean stateful) {
        super(fn_spec, stateful);
    }

    public boolean isRemove(FlowProcess flow_process, FilterCall filter_call) {
        ISeq fn_args_seq = Util.coerceFromTuple(filter_call.getArguments().getTuple());
        return !Util.truthy(applyFunction(fn_args_seq));
    }
}
