package jcascalog;

import clojure.lang.Keyword;

public class Option {
    public static Object SORT = Keyword.intern("sort");
    public static Object REVERSE = Keyword.intern("reverse");
    public static Object TRAP = Keyword.intern("trap");
    public static Object DISTINCT = Keyword.intern("distinct");
    
    public static Predicate distinct(boolean shouldDistinct) {
        return new Predicate(DISTINCT, new Fields(shouldDistinct));
    }

    public static Predicate sort(Fields sortFields) {
        return new Predicate(SORT, sortFields);
    }    

    public static Predicate reverse(boolean shouldReverse) {
        return new Predicate(REVERSE, new Fields(shouldReverse));
    }    

    public static Predicate trap(Object trapTap) {
        return new Predicate(TRAP, new Fields(trapTap));
    }
}
