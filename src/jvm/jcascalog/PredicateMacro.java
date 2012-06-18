package jcascalog;

import java.util.List;

public interface PredicateMacro {
    List<Predicate> getPredicates(Fields inFields, Fields outFields);
}
