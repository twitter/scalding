package jcascalog.op;

import java.util.Arrays;
import jcascalog.ClojureOp;

public class Limit extends ClojureOp {
    public Limit(int amt) {
        super("cascalog.ops", "limit", Arrays.asList((Object)amt));
    }
}
