package jcascalog.op;

import java.util.Arrays;
import jcascalog.ClojureOp;

public class FixedSample extends ClojureOp {
    public FixedSample(int amt) {
        super("cascalog.ops", "fixed-sample-agg", Arrays.asList((Object)amt));
    }
}
