package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe

class SourceTrackingJob(args : Args) extends Job(args) {

  Tracking.init(args)

  def TrackedFileSource(o : FileSource, s : FileSource) : TrackedFileSource = {
    new TrackedFileSource(o, s, args)
  }
  
  override def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    Tracking.tracking.onFlowComplete(flowDef, mode)
    // Sources are good, now connect the flow:
    mode.newFlowConnector(config).connect(flowDef)
  }
}
