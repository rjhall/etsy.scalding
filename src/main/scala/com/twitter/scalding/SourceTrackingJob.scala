package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe

class SourceTrackingJob(args : Args) extends Job(args) {

   override def buildFlow(implicit mode : Mode) = {
     validateSources(mode)
     SourceTracking.writeOutputs(mode, flowDef)
     // Sources are good, now connect the flow:
     mode.newFlowConnector(config).connect(flowDef)
   }
}
