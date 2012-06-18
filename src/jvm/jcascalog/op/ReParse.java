package jcascalog.op;

import java.util.Arrays;
import java.util.regex.Pattern;
import jcascalog.ClojureOp;

public class ReParse extends ClojureOp {
    public ReParse(Pattern pattern) {
        super("cascalog.ops", "re-parse", Arrays.asList((Object)pattern));
    }
}
