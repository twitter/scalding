package jcascalog;

import java.util.ArrayList;
import java.util.List;

public class Fields extends ArrayList<Object> {
    public Fields(Object... fields) {
        for(Object field: fields) {
            add(field);
        }
    }
    
    public Fields(List<Object> fields) {
        for(Object field: fields) {
            add(field);
        }
    }
}
