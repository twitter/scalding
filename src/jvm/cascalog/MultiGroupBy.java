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

import cascading.flow.hadoop.HadoopCoGroupClosure;
import cascading.operation.BaseOperation;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.joiner.Joiner;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.collect.HadoopSpillableTupleList;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;
import org.apache.log4j.Logger;

public class MultiGroupBy extends SubAssembly {
    public static Logger LOG = Logger.getLogger(MultiGroupBy.class);

    public static interface MultiBuffer extends Serializable {
        void operate(MultiBufferContext context);
    }

    public static class MultiBufferContext {
        JoinerClosure _closure;
        HadoopSpillableTupleList _results = new HadoopSpillableTupleList(10000, (CompressionCodec) null, null);
        int _pipeFieldsSum;

        public MultiBufferContext(JoinerClosure closure, int pipeFieldsSum) {
            _closure = closure;
            _pipeFieldsSum = pipeFieldsSum;
        }

        public int size() {
            return _closure.size();
        }

        public void emit(Tuple result) {
            Tuple ret = new Tuple(((HadoopCoGroupClosure)_closure).getGrouping());
            ret.addAll(result);
            while(ret.size() < _pipeFieldsSum) {
                ret.add(0);
            }
            _results.add(ret);
        }

        public Iterator<Tuple> getArgumentsIterator(int pos) {
            return _closure.getIterator(pos);
        }

        public HadoopSpillableTupleList getResults() {
            return _results;
        }
    }

    public static class MultiBufferExecutor implements Serializable {
        private MultiBuffer _buffer;
        private MultiBufferContext _context;
        private int _pipeFieldsSum;
        private JoinerClosure _closure;

        public MultiBufferExecutor(MultiBuffer buffer, int pipeFieldsSum) {
            _buffer = buffer;
            _pipeFieldsSum = pipeFieldsSum;
        }

        public void setContext(JoinerClosure closure) {
            _closure = closure;
            _context = new MultiBufferContext(closure, _pipeFieldsSum);
        }

        public HadoopSpillableTupleList getResults() {
            return _context.getResults();
        }

        public void operate() {
            ((BaseOperation) _buffer).prepare(_closure.getFlowProcess(), null);
            _buffer.operate(_context);
            ((BaseOperation) _buffer).cleanup(_closure.getFlowProcess(), null);
        }
    }


    protected static class MultiGroupJoiner implements Joiner {

        protected MultiBufferExecutor _buffer;

        public MultiGroupJoiner(int pipeFieldsSum, MultiBuffer buffer) {
            _buffer = new MultiBufferExecutor(buffer, pipeFieldsSum);
        }

        public Iterator<Tuple> getIterator(JoinerClosure closure) {
            _buffer.setContext(closure);
            _buffer.operate();
            final Iterator<Tuple> it = _buffer.getResults().iterator();
            return new Iterator<Tuple>() {

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Tuple next() {
                    return new Tuple(it.next());
                }

                @Override
                public void remove() {
                    it.remove();
                }                
            };
        }

        public int numJoins() {
            return -1;
        }
    }

    public MultiGroupBy(Pipe p0, Pipe p1, Fields groupFields, int pipeFieldsSum, MultiBuffer operation) {
        Pipe[] pipes = new Pipe[] { p0, p1};
        Fields[] fields = new Fields[] {groupFields, groupFields};
        init(pipes, fields, pipeFieldsSum, groupFields, operation);
    }

    public MultiGroupBy(Pipe p0, Fields group0, Pipe p1, Fields group1, int pipeFieldsSum, Fields groupRename, MultiBuffer operation) {
        Pipe[] pipes = new Pipe[] { p0, p1};
        Fields[] fields = new Fields[] {group0, group1};
        init(pipes, fields, pipeFieldsSum, groupRename, operation);
    }

    public MultiGroupBy(Pipe[] pipes, Fields groupFields, int pipeFieldsSum, MultiBuffer operation) {
        Fields[] allGroups = new Fields[pipes.length];
        Arrays.fill(allGroups, groupFields);
        init(pipes, allGroups, pipeFieldsSum, groupFields, operation);
    }

    public MultiGroupBy(Pipe[] pipes, Fields[] groupFields, int pipeFieldsSum, Fields groupingRename, MultiBuffer operation) {
        init(pipes, groupFields, pipeFieldsSum, groupingRename, operation);
    }

    protected void init(Pipe[] pipes, Fields[] groupFields, int pipeFieldsSum, Fields groupingRename, MultiBuffer operation) {
        for(int i=0; i<pipes.length; i++) {
            pipes[i] = new Pipe(UUID.randomUUID().toString(), pipes[i]);
            pipes[i] = new Each(pipes[i], Fields.ALL, new Identity(), Fields.RESULTS);
        }
        Fields resultFields =  Fields.join(groupingRename, ((BaseOperation)operation).getFieldDeclaration());
        if(resultFields.size()>pipeFieldsSum) throw new IllegalArgumentException("Can't have output more than sum of input pipes since this is a hack!");
        // unfortunately, need to hack around CoGroup validation stuff since cascading assumes it will return #fields=sum of input pipes
        Fields fake = new Fields();
        fake = fake.append(resultFields);
        int i=0;
        while(fake.size() < pipeFieldsSum) {
            fake = fake.append(new Fields("__" + i));
            i++;
        }
        Pipe result = new CoGroup(pipes, groupFields, fake, new MultiGroupJoiner(pipeFieldsSum, operation));
        result = new Each(result, resultFields, new Identity());
        setTails(result);
    }
}
