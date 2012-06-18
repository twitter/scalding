package cascalog.ops;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

import java.util.Random;

public class RandLong extends CascalogFunction {
    long seed;
    Random rand;

    public RandLong() {
        this.seed = new Random().nextLong();
    }

    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        this.rand = new Random(seed + flowProcess.getCurrentSliceNum());
    }

    public void operate(FlowProcess flow_process, FunctionCall fn_call) {
        fn_call.getOutputCollector().add(new Tuple(rand.nextLong()));
    }
}
