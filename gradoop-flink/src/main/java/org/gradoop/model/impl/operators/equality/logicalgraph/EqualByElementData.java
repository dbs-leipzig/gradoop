package org.gradoop.model.impl.operators.equality.logicalgraph;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryGraphToValueOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.operators.equality.EqualityBase;
import org.gradoop.model.impl.operators.equality.functions.LabelAppender;
import org.gradoop.model.impl.operators.equality.functions.SortAndConcatLabels;
import org.gradoop.model.impl.operators.equality.functions.SourceVertexLabelAppender;
import org.gradoop.model.impl.operators.equality.functions.EdgeDataLabeler;
import org.gradoop.model.impl.operators.equality.functions.TargetVertexLabelAppender;
import org.gradoop.model.impl.operators.equality.functions.VertexDataLabeler;
import org.gradoop.model.impl.operators.equality.tuples.EdgeDataLabel;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;

public class EqualByElementData
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends EqualityBase implements BinaryGraphToValueOperator<V, E, G, Boolean> {

  @Override
  public DataSet<Boolean> execute(LogicalGraph<V, E, G> firstGraph,
    LogicalGraph<V, E, G> secondGraph) {

    DataSet<DataLabel> firstGraphLabel = labelGraph(firstGraph);
    DataSet<DataLabel> secondGraphLabel = labelGraph(secondGraph);

    return ensureBooleanSetIsNotEmpty(
      firstGraphLabel
        .cross(secondGraphLabel)
        .with(new Equals<DataLabel>())
    );
    }

  private DataSet<DataLabel> labelGraph(LogicalGraph<V, E, G> graph) {

    DataSet<DataLabel> vertexLabels = graph.getVertices()
      .map(new VertexDataLabeler<V>());

    DataSet<EdgeDataLabel> edgeLabels = graph.getEdges()
      .map(new EdgeDataLabeler<E>())
      .groupBy(0, 1)
      .reduceGroup(new SortAndConcatLabels<EdgeDataLabel>());

    DataSet<DataLabel> outgoingEdgeLabels = edgeLabels
      .join(vertexLabels)
      .where(1).equalTo(0)
      .with(new TargetVertexLabelAppender())
      .groupBy(0)
      .reduceGroup(new SortAndConcatLabels<DataLabel>());

    DataSet<DataLabel> incomingEdgeLabels = edgeLabels
      .join(vertexLabels)
      .where(0).equalTo(0)
      .with(new SourceVertexLabelAppender())
      .groupBy(0)
      .reduceGroup(new SortAndConcatLabels<DataLabel>());

    return vertexLabels
      .leftOuterJoin(outgoingEdgeLabels)
      .where(0).equalTo(0)
      .with(new LabelAppender())
      .leftOuterJoin(incomingEdgeLabels)
      .where(0).equalTo(0)
      .with(new LabelAppender())
      .reduceGroup(new SortAndConcatLabels<DataLabel>());
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
    }
}
