package org.gradoop.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.operators.cam.CanonicalAdjacencyMatrix;
import org.gradoop.model.impl.operators.cam.functions.EdgeLabeler;
import org.gradoop.model.impl.operators.cam.functions.GraphHeadLabeler;
import org.gradoop.model.impl.operators.cam.functions.VertexLabeler;

public class CollectionEquality
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements BinaryCollectionToValueOperator<G, V, E, Boolean> {

  private final CanonicalAdjacencyMatrix<G, V, E> canonicalAdjacencyMatrix;

  public CollectionEquality(
    GraphHeadLabeler<G> graphHeadLabeler,
    VertexLabeler<V> vertexLabeler,
    EdgeLabeler<E> egeLabelingFunction) {

    this.canonicalAdjacencyMatrix = new CanonicalAdjacencyMatrix<>(
      graphHeadLabeler, vertexLabeler, egeLabelingFunction
    );
  }

  @Override
  public DataSet<Boolean> execute(GraphCollection<G, V, E> firstCollection,
    GraphCollection<G, V, E> secondCollection) {
    return Equals.cross(canonicalAdjacencyMatrix.execute(firstCollection),
      canonicalAdjacencyMatrix.execute(secondCollection));
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
