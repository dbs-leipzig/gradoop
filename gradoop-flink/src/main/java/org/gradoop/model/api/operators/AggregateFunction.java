package org.gradoop.model.api.operators;


import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.model.LogicalGraph;

public interface AggregateFunction<N extends Number,
  G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  DataSet<N> execute(LogicalGraph<G, V, E> graph);

}
