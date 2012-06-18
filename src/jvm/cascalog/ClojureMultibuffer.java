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

import cascading.tuple.Fields;
import cascalog.MultiGroupBy.MultiBuffer;
import cascalog.MultiGroupBy.MultiBufferContext;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import clojure.lang.RT;

import java.util.ArrayList;
import java.util.List;

public class ClojureMultibuffer extends ClojureCascadingBase implements MultiBuffer {

    public ClojureMultibuffer(Fields out_fields, Object[] fn_spec, boolean stateful) {
        super(out_fields, fn_spec, stateful);
    }

    public void operate(MultiBufferContext context) {
        List inputTuples = new ArrayList();
        for (int i = 0; i < context.size(); i++) {
            inputTuples.add(IteratorSeq.create(new RegularTupleSeqConverter(context.getArgumentsIterator(i))));
        }

        ISeq result_seq = RT.seq(applyFunction(RT.seq(inputTuples)));
        while (result_seq != null) {
            Object obj = result_seq.first();
            context.emit(Util.coerceToTuple(obj));
            result_seq = result_seq.next();
        }
    }
}
