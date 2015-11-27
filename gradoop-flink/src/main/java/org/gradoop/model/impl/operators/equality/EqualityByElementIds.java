package org.gradoop.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryGraphToValueOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.ToGradoopIdSet;
import org.gradoop.model.impl.functions.bool.And;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * Created by peet on 17.11.15.
 */
public class EqualityByElementIds
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends EqualityBase implements BinaryGraphToValueOperator<V, E, G, Boolean> {

  @Override
  public DataSet<Boolean> execute(LogicalGraph<G, V, E> firstGraph,
    LogicalGraph<G, V, E> secondGraph) {

    DataSet<GradoopIdSet> firstGraphVertexIds = firstGraph.getVertices()
      .map(new Id<V>())
      .reduceGroup(new ToGradoopIdSet());

    DataSet<GradoopIdSet> secondGraphVertexIds = secondGraph.getVertices()
      .map(new Id<V>())
      .reduceGroup(new ToGradoopIdSet());

    DataSet<GradoopIdSet> firstGraphEdgeIds = firstGraph.getEdges()
      .map(new Id<E>())
      .reduceGroup(new ToGradoopIdSet());

    DataSet<GradoopIdSet> secondGraphEdgeIds = secondGraph.getEdges()
      .map(new Id<E>())
      .reduceGroup(new ToGradoopIdSet());

    DataSet<Boolean> equalVertices = firstGraphVertexIds
      .cross(secondGraphVertexIds)
      .with(new Equals<GradoopIdSet>());

    DataSet<Boolean> equalEdges = firstGraphEdgeIds
      .cross(secondGraphEdgeIds)
      .with(new Equals<GradoopIdSet>());

    return equalVertices
      .cross(equalEdges)
      .with(new And());
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
