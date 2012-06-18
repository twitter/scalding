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

import cascading.tuple.TupleEntry;
import clojure.lang.ISeq;

import java.util.Iterator;

public class TupleSeqConverter implements Iterator<ISeq> {
    private Iterator<TupleEntry> _tuples;

    public TupleSeqConverter(Iterator<TupleEntry> tuples) {
        _tuples = tuples;
    }

    public boolean hasNext() {
        return _tuples.hasNext();
    }

    public ISeq next() {
        return Util.coerceFromTuple(_tuples.next().getTuple());
    }

    public void remove() {
        _tuples.remove();
    }
}
