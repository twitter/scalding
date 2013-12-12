package com.twitter.scalding

import cascading.flow.FlowProcess

object RichFlowProcess extends java.io.Serializable {
    private var _flowProcess : ThreadLocal[FlowProcess[_]] = new ThreadLocal()

    def setFlowProcess(fp : FlowProcess[_]) =
        if (_flowProcess.get == null) {
          _flowProcess.set(fp)
        }

    def flowProcess() : FlowProcess[_] = {
        assert(_flowProcess != null, "flow process is undefined: your operation needs to set the flow process")
        
        _flowProcess.get
    }

    def incrementCounter(group : String, counter : String, amount : Long) =
        flowProcess.increment(group, counter, amount)

    def keepAlive() =
        flowProcess.keepAlive()
}