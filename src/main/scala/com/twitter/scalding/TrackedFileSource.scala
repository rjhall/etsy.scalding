package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.{Fields, Tuple, TupleEntry}

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

class TrackedFileSource(val original : FileSource, val subset : FileSource, args : Args) extends FileSource {

  val use_sources = args.boolean("use_sources")

  // To allow for use in a map, as in the testing code.
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
    if(use_sources)
      subset.hdfsPaths
    else
      original.hdfsPaths
  }

  override def localPath : String = {
    if(use_sources)
      subset.localPath
    else
      original.localPath
  }

  override def writeFrom(pipe : Pipe)(implicit flowDef : FlowDef, mode : Mode) = {
    throw new RuntimeException("Source: (" + toString + ") doesn't support writing")
    pipe
  }

  override def read(implicit flowDef : FlowDef, mode : Mode, tracking : Tracking) : Pipe = {
    tracking.afterRead(this, super.read)
  }

  override def toString : String = {
    "ORIGINAL: " + original.toString + " SUBSET: " + subset.toString
  }
}

