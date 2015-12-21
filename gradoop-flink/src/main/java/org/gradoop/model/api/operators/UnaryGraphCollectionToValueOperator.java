package org.gradoop.model.api.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;

public interface UnaryGraphCollectionToValueOperator
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge, T> {

  DataSet<T> execute(GraphCollection<G, V, E> collection);
}
