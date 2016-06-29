package org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.io.impl.tlf.tuples.TLFVertex;


public class TLFVertexLabels implements FlatMapFunction<TLFGraph, String> {

  @Override
  public void flatMap(TLFGraph tlfGraph, Collector<String> collector) throws
    Exception {

    for (TLFVertex vertex : tlfGraph.getGraphVertices()) {
      collector.collect(vertex.getLabel());
    }
  }
}
