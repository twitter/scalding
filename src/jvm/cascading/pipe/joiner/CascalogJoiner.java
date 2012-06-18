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

package cascading.pipe.joiner;

import cascading.tuple.Tuple;

import java.util.Iterator;
import java.util.List;


public class CascalogJoiner implements Joiner {
    public static enum JoinType {
        INNER,
        OUTER,
        EXISTS;
    }

    private final List<JoinType> joins;

    public CascalogJoiner(List<JoinType> joins) {
        this.joins = joins;
    }

    @Override public Iterator<Tuple> getIterator(JoinerClosure closure) {
        return new JoinIterator(closure);
    }

    public int numJoins() {
        return joins.size()-1;
    }

    protected class JoinIterator extends OuterJoin.JoinIterator {
        public JoinIterator(JoinerClosure closure) {
            super(closure);
        }

        @Override protected boolean isOuter(int i) {
            return joins.get(i)!=JoinType.INNER && super.isOuter( i );
        }

        @Override protected Iterator getIterator(int i) {
            if(joins.get(i)==JoinType.EXISTS) {
                final boolean isEmpty = closure.isEmpty(i);
                final Iterator wrapped = super.getIterator(i);
                return new Iterator() {
                    private boolean emittedOne = false;

                    public boolean hasNext() {
                        return !emittedOne && wrapped.hasNext();
                    }

                    public Object next() {
                        if(emittedOne)
                            throw new RuntimeException("Shouldn't be accessing outerjoin_first more than once");
                        emittedOne = true;
                        Tuple t = (Tuple) wrapped.next();
                        Tuple ret = new Tuple();
                        for(int i=0; i<t.size(); i++) {
                            ret.add(!isEmpty);
                        }
                        return ret;
                    }

                    public void remove() {
                        //not implemented
                    }

                };
            } else {
                return super.getIterator(i);
            }
        }
    }
}
