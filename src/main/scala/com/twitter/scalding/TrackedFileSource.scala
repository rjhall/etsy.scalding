package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.{Fields, Tuple, TupleEntry}

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

class TrackedFileSource(val original : FileSource, val subset : FileSource) extends FileSource {

  override def equals(a : Any) = {
    a.isInstanceOf[TrackedFileSource] && 
      a.asInstanceOf[TrackedFileSource].original == original &&
      a.asInstanceOf[TrackedFileSource].subset == subset
  }
  
  override def hashCode = original.hashCode

  override def localScheme : LocalScheme = {
    original.localScheme
  }

  override def hdfsScheme : Scheme[JobConf,RecordReader[_,_],OutputCollector[_,_],_,_] = {
    original.hdfsScheme
  }

  override def hdfsPaths : Iterable[String] = {
    if(SourceTracking.use_sources)
      subset.hdfsPaths
    else
      original.hdfsPaths
  }

  override def localPath : String = {
    if(SourceTracking.use_sources)
      subset.localPath
    else
      original.localPath
  }

  override def writeFrom(pipe : Pipe)(implicit flowDef : FlowDef, mode : Mode) = {
    throw new RuntimeException("Source: (" + toString + ") doesn't support writing")
    pipe
  }

  override def read(implicit flowDef : FlowDef, mode : Mode) : Pipe = { 
    if(SourceTracking.track_sources)
      prepareSource
    else
      super.read
  } 

  def prepareSource(implicit flowDef : FlowDef, mode : Mode) : Pipe = {
    // Register file for tracking.
    SourceTracking.register(original, subset)
    // Add hidden field for source data tracking. 
    import Dsl._
    val fields = hdfsScheme.getSourceFields
    // Use the string representation of the original source file as its identifier.
    val fp = original.toString
    super.read.map(fields -> SourceTracking.sourceTrackingField){ te : TupleEntry => Map(fp -> List[Tuple](te.getTuple)) }
  }

  override def toString : String = {
    "ORIGINAL: " + original.toString + " SUBSET: " + subset.toString
  }
}

