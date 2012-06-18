package jcascalog.op;

import java.util.Arrays;
import jcascalog.ClojureOp;

public class LimitRank extends ClojureOp {
    public LimitRank(int amt) {
        super("cascalog.ops", "limit-rank", Arrays.asList((Object)amt));
    }
}
