package com.twitter.scalding

import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry

import org.specs._
import java.lang.{Integer => JInt}

class SourceTrackingMapJob(args : Args) extends Job(args) {
  Tsv("input", ('x,'y))
  .mapTo(('x, 'y) -> 'z){ x : (Int, Int) => x._1 + x._2 }
  .write(Tsv("output"))
}

class SourceTrackingMapTest extends Specification with TupleConversions {
  import Dsl._
  "Running with --write_sources" should {
    //Set up the job:
    "correctly track sources" in {
      JobTest("com.twitter.scalding.SourceTrackingMapJob")
        .arg("write_sources", "true")
        .arg("source_output_prefix", "bar")
        .source(Tsv("input", ('x, 'y)), List(("0","1"), ("1","3"), ("2","9")))
        .sink[(Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(3)
          unordered((1)) must be_==(true)
          unordered((4)) must be_==(true)
          unordered((11)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("bar/input")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(3)
          unordered((0,1)) must be_==(true)
          unordered((1,3)) must be_==(true)
          unordered((2,9)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}

class UseSourceTrackingTest extends Specification with TupleConversions {
  import Dsl._
  "Running with --use_sources" should {
    //Set up the job:
    "correctly use provided sources" in {
      JobTest("com.twitter.scalding.SourceTrackingMapJob")
        .arg("use_sources", "true")
        .arg("source_output_prefix", "foo")
        .source(Tsv("foo/input", ('x, 'y)), List(("1","1"), ("1","3"), ("2","9")))
        .sink[(Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(3)
          unordered((2)) must be_==(true)
          unordered((4)) must be_==(true)
          unordered((11)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}
