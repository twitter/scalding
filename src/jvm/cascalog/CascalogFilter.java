package cascalog;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;

public abstract class CascalogFilter extends BaseOperation implements Filter {

    public boolean isRemove(FlowProcess process, FilterCall filterCall) {
        return !isKeep(process, filterCall);
    }
    
    public abstract boolean isKeep(FlowProcess process, FilterCall filterCall);
}
