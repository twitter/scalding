package cascalog.test;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogFunction;
import clojure.lang.Numbers;


public class DoubleOp extends CascalogFunction {

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall fnCall) {
        TupleEntry args = fnCall.getArguments();
        Number n = (Number) args.get(0);
        fnCall.getOutputCollector().add(new Tuple(Numbers.multiply(n, 2)));
    }
}
