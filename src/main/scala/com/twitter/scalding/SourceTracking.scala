/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields

object SourceTracking {

  var track_sources : Boolean = false;
  var use_sources : Boolean = false;

  var sources : Map[String, Source] = Map[String,Source]()
  var tail_pipes : Map[String, Pipe] = Map[String,Pipe]()
  var fields : Map[String, Fields] = Map[String,Fields]()

  var source_output_prefix : String = "sample"
  var source_tracking_field_name : String = "__souce_data__"

  def init(args : Args) : Unit = {
    track_sources = args.boolean("write_sources")
    use_sources = args.boolean("use_sources")
    source_tracking_field_name = args.getOrElse("source_tracking_field_name", "__source_data__")
    source_output_prefix = args.getOrElse("source_output_prefix", "sample");
  }

  def reset : Unit = { 
    track_sources = false;
    use_sources = false;
    sources = Map[String,Source]()
    tail_pipes = Map[String,Pipe]()
    fields = Map[String,Fields]()
  }

  def sourceTrackingField : Fields = new Fields(source_tracking_field_name)

  def getOpenSource(fp : String) : Source = {
    sources.getOrElse(fp, null)
  }

  def getFields(fp : String) : Fields = {
    fields.getOrElse(fp, null)
  }

  def openForWrite(fp : String, sourcefile : FileSource, field : Fields) : Unit = {
    if(!sources.contains(fp)) {
      sources += (fp -> sourcefile.getOutputForSourceDataTracking(fp))
      fields += (fp -> field)
    }
  }

  def writePipe(fp : String, pipe : Pipe) : Unit = {
    if(tail_pipes.contains(fp)) {
      tail_pipes += (fp -> (RichPipe(pipe) ++ tail_pipes(fp)))
    } else {
      tail_pipes += (fp -> pipe)
    }
  }

  def writeOutputs(mode : Mode, flowDef : FlowDef) : Unit = {
    track_sources = false;
    tail_pipes.foreach{ x : (String, Pipe) =>
      val so = getOpenSource(x._1)
      so.writeFrom(RichPipe(x._2).unique(getFields(x._1)))(flowDef, mode)
    }
  }

  def sourceFilePath(path : String) : String = {
    (source_output_prefix + "/" + path).replaceAll("//", "/").replaceAll("/\\*", "")
  }

}
