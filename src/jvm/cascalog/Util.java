/*
    Copyright 2010 Nathan Marz
 
    Project and contact information: http://www.cascalog.org/ 

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
   
        http://www.apache.org/licenses/LICENSE-2.0
   
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package cascalog;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import clojure.lang.*;

import java.io.FileNotFoundException;
import java.util.*;

public class Util {
    static Var require = RT.var("clojure.core", "require");
    static Var symbol = RT.var("clojure.core", "symbol");

    public static ISeq cat(ISeq s1, ISeq s2) {
        if (s1 == null || RT.seq(s1) == null) { return s2; }
        return cat(s1.next(), s2).cons(s1.first());
    }

    public static Throwable getRootCause(Throwable e) {
        Throwable rootCause = e;
        Throwable nextCause = rootCause.getCause();

        while (nextCause != null) {
            rootCause = nextCause;
            nextCause = rootCause.getCause();
        }
        return rootCause;
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public static void tryRequire(String ns_name) {
        try {
            require.invoke(symbol.invoke(ns_name));
        } catch (Exception e) {

            //if playing from the repl and defining functions, file won't exist
            Throwable rootCause = getRootCause(e);

            boolean fileNotFound = (rootCause instanceof FileNotFoundException);
            boolean nsFileMissing = e.getMessage().contains(ns_name + ".clj on classpath");

            if (!(fileNotFound && nsFileMissing))
                throw new RuntimeException(e);
        }
    }
    
    public static Var getVar(String ns_name, String fn_name) {
        tryRequire(ns_name);
        return RT.var(ns_name, fn_name);        
    }
    
    public static IFn bootSimpleFn(String ns_name, String fn_name) {
        return getVar(ns_name, fn_name);
    }

    public static IFn bootFn(Object[] fn_spec) {
        String ns_name = (String) fn_spec[0];
        String fn_name = (String) fn_spec[1];
        IFn simple_fn = bootSimpleFn(ns_name, fn_name);
        if (fn_spec.length == 2) {
            return simple_fn;
        } else {
            ISeq hof_args = ArraySeq.create(fn_spec).next().next();
            try {
                return (IFn) simple_fn.applyTo(hof_args);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static ISeq coerceToSeq(Object o) {
        if (o instanceof List) {
            return RT.seq(o);
        } else {
            return RT.list(o);
        }
    }

    public static List coerceToList(Object o) {
        if (o instanceof List) {
            return (List) o;
        } else {
            return Arrays.asList(o);
        }
    }    
    
    public static IteratorSeq coerceFromTuple(Tuple tuple) {
        return IteratorSeq.create(tuple.iterator());
    }

    public static IteratorSeq coerceFromTuple(TupleEntry tuple) {
        return coerceFromTuple(tuple.getTuple());
    }

    public static Tuple coerceToTuple(Object obj) {
        if (obj instanceof List) {
            Object[] arr = ((List) obj).toArray();
            return new Tuple(arr);
        } else {
            return new Tuple(obj);
        }
    }

    public static List seqToList(ISeq s) {
        s = RT.seq(s);
        List<Object> ret = new ArrayList<Object>();
        while (s != null) {
            ret.add(s.first());
            s = s.next();
        }
        return ret;
    }

    // TODO: convert to RT.booleanCast
    public static boolean truthy(Object obj) {
        return ((obj != null) && (!Boolean.FALSE.equals(obj)));
    }
    
    public static void tupleIntoList(List<Object> ret, Tuple tuple) {
        for (Object aTuple : tuple) {
            ret.add(aTuple);
        }
    }
    
    public static List<Object> tupleToList(Tuple tuple) {
        List<Object> ret = new ArrayList<Object>();
        tupleIntoList(ret, tuple);
        return ret;
    }
    
    public static List<Object> tupleToList(TupleEntry tuple) {
        return tupleToList(tuple.getTuple());
    }
    
    public static List<Object> toList(Object[] arr) {
        List<Object> ret = new ArrayList<Object>();
        Collections.addAll(ret, arr);
        return ret;
    }
}
