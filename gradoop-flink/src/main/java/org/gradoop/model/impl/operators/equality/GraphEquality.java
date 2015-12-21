package org.gradoop.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryGraphToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.cam.functions.EdgeLabeler;
import org.gradoop.model.impl.operators.cam.functions.GraphHeadLabeler;
import org.gradoop.model.impl.operators.cam.functions.VertexLabeler;


public class GraphEquality
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements BinaryGraphToValueOperator<G, V, E, Boolean> {

  private final CollectionEquality<G, V, E> collectionEquality;

  public GraphEquality(
    GraphHeadLabeler<G> graphHeadLabeler,
    VertexLabeler<V> vertexLabeler,
    EdgeLabeler<E> egeLabelingFunction) {

    this.collectionEquality = new CollectionEquality<>(
      graphHeadLabeler, vertexLabeler, egeLabelingFunction
    );
  }

  @Override
  public DataSet<Boolean> execute(
    LogicalGraph<G, V, E> firstGraph, LogicalGraph<G, V, E> secondGraph) {
    return collectionEquality.execute(
      GraphCollection.fromGraph(firstGraph),
      GraphCollection.fromGraph(secondGraph)
    );
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
