package jcascalog.example;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;

public class Split extends CascalogFunction {
    public void operate(FlowProcess flowProcess, FunctionCall fnCall) {
        String sentence = fnCall.getArguments().getString(0);
        for(String word: sentence.split(" ")) {
            fnCall.getOutputCollector().add(new Tuple(word));
        }
    }
}
