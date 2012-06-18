package cascalog.test;

import cascading.flow.FlowProcess;
import cascading.operation.OperationCall;
import cascalog.ParallelAgg;
import clojure.lang.Numbers;
import java.util.Arrays;
import java.util.List;

public class MultiplyAgg implements ParallelAgg {

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    }

    @Override
    public List<Object> init(List<Object> input) {
        return input;
    }

    @Override
    public List<Object> combine(List<Object> val1, List<Object> val2) {
        Number n1 = (Number) val1.get(0);
        Number n2 = (Number) val2.get(0);
        return Arrays.asList((Object) Numbers.multiply(n1, n2));
    }
    
}
