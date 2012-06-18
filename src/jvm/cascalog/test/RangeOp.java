package cascalog.test;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogFunction;


public class RangeOp extends CascalogFunction {

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall fnCall) {
        TupleEntry args = fnCall.getArguments();
        Number n = (Number) args.get(0);
        for(int i=1; i<=n.intValue(); i++) {
            fnCall.getOutputCollector().add(new Tuple(i));
        }
    }
    
}
