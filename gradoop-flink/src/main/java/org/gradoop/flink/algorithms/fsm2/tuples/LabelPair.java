package org.gradoop.flink.algorithms.fsm2.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

public class LabelPair extends Tuple2<String, String> {

  public LabelPair(String edgeLabel, String vertexLabel) {
    super(edgeLabel, vertexLabel);
  }

  public String getVertexLabel() {
    return this.f1;
  }

  public String getEdgeLabel() {
    return this.f0;
  }
}
