package org.gradoop.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CrossOperator;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryGraphToValueOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.bool.And;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.functions.bool.Or;
import org.gradoop.model.impl.operators.equality.functions.LabelAppender;
import org.gradoop.model.impl.operators.equality.functions.SortAndConcatLabels;
import org.gradoop.model.impl.operators.equality.functions.SourceLabelAppender;
import org.gradoop.model.impl.operators.equality.functions.EdgeDataLabeler;
import org.gradoop.model.impl.operators.equality.functions.TargetLabelAppender;
import org.gradoop.model.impl.operators.equality.functions.VertexDataLabeler;
import org.gradoop.model.impl.operators.equality.tuples.EdgeDataLabel;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;

/**
 * Two graph are equal,
 * if there exists an isomorphism based on element label and property equality.
 *
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class EqualityByElementData
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends EqualityBase implements BinaryGraphToValueOperator<V, E, G, Boolean> {

  @Override
  public DataSet<Boolean> execute(LogicalGraph<G, V, E> firstGraph,
    LogicalGraph<G, V, E> secondGraph) {

    DataSet<DataLabel> firstGraphLabel = labelGraph(firstGraph);
    DataSet<DataLabel> secondGraphLabel = labelGraph(secondGraph);

    return Or.union(
      And.cross(firstGraph.isEmpty(), secondGraph.isEmpty()),
      Equals.cross(firstGraphLabel, secondGraphLabel)
    );
  }

  /**
   * Returns an 1-element dataset representing the canonical label of a graph.
   *
   * @param graph input graph
   * @return canonical label
   */
  private DataSet<DataLabel> labelGraph(LogicalGraph<G, V, E> graph) {

    DataSet<DataLabel> vertexLabels = graph.getVertices()
      .map(new VertexDataLabeler<V>());

    DataSet<EdgeDataLabel> edgeLabels = graph.getEdges()
      .map(new EdgeDataLabeler<E>())
      .groupBy(1, 2)
      .reduceGroup(new SortAndConcatLabels<EdgeDataLabel>());

    DataSet<DataLabel> outgoingEdgeLabels = edgeLabels
      .join(vertexLabels)
      .where(2).equalTo(1)
      .with(new TargetLabelAppender())
      .groupBy(1)
      .reduceGroup(new SortAndConcatLabels<DataLabel>());

    DataSet<DataLabel> incomingEdgeLabels = edgeLabels
      .join(vertexLabels)
      .where(1).equalTo(1)
      .with(new SourceLabelAppender())
      .groupBy(1)
      .reduceGroup(new SortAndConcatLabels<DataLabel>());

    return vertexLabels
      .leftOuterJoin(outgoingEdgeLabels)
      .where(1).equalTo(1)
      .with(new LabelAppender())
      .leftOuterJoin(incomingEdgeLabels)
      .where(1).equalTo(1)
      .with(new LabelAppender())
      .reduceGroup(new SortAndConcatLabels<DataLabel>());
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
