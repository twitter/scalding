package jcascalog;

import cascalog.Util;
import java.util.Arrays;
import java.util.List;

public class Subquery {
    Object _compiled;
    
    public Subquery(Fields outFields, Predicate... preds) {
        this(outFields, Arrays.asList(preds));
    }

    public Subquery(Fields outFields, List<Predicate> preds) {
        _compiled = Util.bootSimpleFn("cascalog.rules", "build-rule")
                        .invoke(outFields, preds);
    }
    
    public Object getCompiledSubquery() {
        return _compiled;
    }
}
