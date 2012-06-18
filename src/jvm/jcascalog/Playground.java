package jcascalog;

import cascalog.Util;
import clojure.lang.Var;
import java.util.List;

/**
 * Some in-memory datasets to use to play around with Cascalog. You can see 
 * the contents of the datasets in the src/clj/cascalog/playground.clj file.
 */
public class Playground {
    public static List AGE = getDataset("age");
    public static List GENDER = getDataset("gender");
    public static List FOLLOWS = getDataset("follows");
    public static List FULL_NAMES = getDataset("full-names");
    public static List LOCATION = getDataset("location");
    public static List INTEGER = getDataset("integer");
    public static List SENTENCE = getDataset("sentence");
    
    
    
    private static List getDataset(String name) {
        Var v = Util.getVar("cascalog.playground", name);
        return (List) v.deref();
    }
}
