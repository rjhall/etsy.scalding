package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields

object SourceTracking {

  var track_sources : Boolean = false;
  var use_sources : Boolean = false;

  // Original data source -> output (subset) data source.
  var sources : Map[FileSource, FileSource] = Map[FileSource, FileSource]()

  // Original data source -> pipe which accumulates output.
  var tail_pipes : Map[FileSource, Pipe] = Map[FileSource,Pipe]()

  var source_tracking_field_name : String = "__source_data__"

  def init(args : Args) : Unit = {
    track_sources = args.boolean("write_sources")
    use_sources = args.boolean("use_sources")
    source_tracking_field_name = args.getOrElse("source_tracking_field_name", "__source_data__")
  }

  def reset : Unit = { 
    track_sources = false;
    use_sources = false;
    sources = Map[FileSource, FileSource]()
    tail_pipes = Map[FileSource,Pipe]()
  }

  def sourceTrackingField : Fields = new Fields(source_tracking_field_name)

  def getOpenSource(orig : FileSource) : Source = {
    sources.getOrElse(orig, null)
  }

  def getFields(orig : FileSource) : Fields = {
    println(orig.hdfsScheme.getSourceFields)
    orig.hdfsScheme.getSourceFields
  }

  def register(orig : FileSource, subset : FileSource) : Unit = {
    if(!sources.contains(orig)) {
      sources += (orig -> subset)
    } else if(sources(orig) != subset) {
      sys.error("asdf");
    }
  }

  def writePipe(orig : FileSource, pipe : Pipe) : Unit = {
    if(tail_pipes.contains(orig)) {
      tail_pipes += (orig -> (RichPipe(pipe) ++ tail_pipes(orig)))
    } else {
      tail_pipes += (orig -> pipe)
    }
  }

  def writeOutputs(mode : Mode, flowDef : FlowDef) : Unit = {
    track_sources = false;
    tail_pipes.foreach{ x : (FileSource, Pipe) =>
      val so = getOpenSource(x._1)
      so.writeFrom(RichPipe(x._2).unique(getFields(x._1)))(flowDef, mode)
    }
  }

}
