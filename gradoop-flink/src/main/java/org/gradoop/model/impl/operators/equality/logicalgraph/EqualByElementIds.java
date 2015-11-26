package org.gradoop.model.impl.operators.equality.logicalgraph;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryGraphToValueOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.ToGradoopIds;
import org.gradoop.model.impl.functions.bool.And;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.functions.isolation.ElementId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.operators.equality.EqualityBase;

/**
 * Created by peet on 17.11.15.
 */
public class EqualByElementIds
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends EqualityBase implements BinaryGraphToValueOperator<V, E, G, Boolean> {

  @Override
  public DataSet<Boolean> execute(LogicalGraph<G, V, E> firstGraph,
    LogicalGraph<G, V, E> secondGraph) {

    DataSet<GradoopIdSet> firstGraphVertexIds = firstGraph.getVertices()
      .map(new ElementId<V>())
      .reduceGroup(new ToGradoopIds());

    DataSet<GradoopIdSet> secondGraphVertexIds = secondGraph.getVertices()
      .map(new ElementId<V>())
      .reduceGroup(new ToGradoopIds());

    DataSet<GradoopIdSet> firstGraphEdgeIds = firstGraph.getEdges()
      .map(new ElementId<E>())
      .reduceGroup(new ToGradoopIds());

    DataSet<GradoopIdSet> secondGraphEdgeIds = secondGraph.getEdges()
      .map(new ElementId<E>())
      .reduceGroup(new ToGradoopIds());

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
