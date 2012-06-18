package jcascalog;

import clojure.lang.Keyword;
import java.util.ArrayList;
import java.util.List;

public class Predicate {
    List<Object> _fieldsDeclaration = new ArrayList<Object>();
    Object _op;
    
    public Predicate(Object op, Fields defaultFields) {
        _op = op;
        _fieldsDeclaration.addAll(defaultFields);
    }

    public Predicate(Object op, Fields infields, Fields outFields) {
        _op = op;
        _fieldsDeclaration.addAll(infields);
        _fieldsDeclaration.add(Keyword.intern(">"));
        _fieldsDeclaration.addAll(outFields);
    }
    
    public List<Object> toRawCascalogPredicate() {
        List<Object> pred = new ArrayList<Object>();
        pred.add(_op); // the op
        pred.add(_fieldsDeclaration);
        return pred;
    }    
}
