package cascalog.hadoop;

import cascading.tuple.Hasher;
import clojure.lang.Util;

import java.io.Serializable;
import java.util.Comparator;

/** User: sritchie Date: 12/12/11 Time: 3:23 PM */
public class DefaultComparator implements Comparator, Hasher<Object>, Serializable {

    public int compare(Object o1, Object o2) {
        return Util.compare(o1, o2);
    }

    private int numericHash(Number x) {
        Class xc = x.getClass();

        if(xc == Long.class
           || xc == Integer.class
           || xc == Short.class
           || xc == Byte.class)
        {
            long lpart = x.longValue();
            return (int) (lpart ^ (lpart >>> 32));
        }
        return x.hashCode();
    }

    public int hashCode(Object o) {
        if (o instanceof Number)
            return numericHash((Number) o);

        return o.hashCode();
    }
}
