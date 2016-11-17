package org.gradoop.flink.algorithms.fsm2.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

public class LabelPair extends Tuple2<String, String> {

  public LabelPair(String edgeLabel, String vertexLabel) {
    super(edgeLabel, vertexLabel);
  }
}
