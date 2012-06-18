package cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;

public class CascalogAggregatorExecutor extends BaseOperation implements Aggregator {
    CascalogAggregator agg;

    public CascalogAggregatorExecutor(Fields outFields, CascalogAggregator agg) {
        super(outFields);
        this.agg = agg;
    }
    
    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        agg.prepare(flowProcess, operationCall);
    }

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
        agg.prepare(flowProcess, operationCall);
    }
    
    public void start(FlowProcess flowProcess, AggregatorCall aggCall) {
        agg.start(flowProcess, aggCall);
    }

    public void aggregate(FlowProcess flowProcess, AggregatorCall aggCall) {
        agg.aggregate(flowProcess, aggCall);
    }

    public void complete(FlowProcess flowProcess, AggregatorCall aggCall) {
        agg.complete(flowProcess, aggCall);
    }
    
}
