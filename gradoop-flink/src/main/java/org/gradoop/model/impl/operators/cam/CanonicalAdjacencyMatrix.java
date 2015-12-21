package org.gradoop.model.impl.operators.cam;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.cam.functions.*;
import org.gradoop.model.impl.operators.cam.tuples.EdgeLabel;
import org.gradoop.model.impl.operators.cam.tuples.GraphHeadLabel;
import org.gradoop.model.impl.operators.cam.tuples.VertexLabel;

public class CanonicalAdjacencyMatrix
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphCollectionToValueOperator<G, V, E, String> {

  private final GraphHeadLabeler<G> graphHeadLabeler;
  private final VertexLabeler<V> vertexLabeler;
  private final EdgeLabeler<E> egeLabelingFunction;

  public CanonicalAdjacencyMatrix(
    GraphHeadLabeler<G> graphHeadLabeler,
    VertexLabeler<V> vertexLabeler,
    EdgeLabeler<E> egeLabelingFunction) {
    this.graphHeadLabeler = graphHeadLabeler;
    this.vertexLabeler = vertexLabeler;
    this.egeLabelingFunction = egeLabelingFunction;
  }

  @Override
  public DataSet<String> execute(GraphCollection<G, V, E> collection) {

    // 1. label graph heads
    DataSet<GraphHeadLabel> graphHeadLabels = collection.getGraphHeads()
      .map(graphHeadLabeler);

    // 2. label vertices
    DataSet<VertexLabel> vertexLabels = collection.getVertices()
      .flatMap(vertexLabeler);

    // 3. label edges
    DataSet<EdgeLabel> edgeLabels = collection.getEdges()
      .flatMap(egeLabelingFunction)
      .groupBy(0, 1, 2)
      .reduceGroup(new MultiEdgeCombiner());

    // 4. extend edge labels by vertex labels

    edgeLabels = edgeLabels
      .join(vertexLabels)
      .where(0, 1).equalTo(0, 1) // graphId,sourceId = graphId,vertexId
      .with(new SourceLabelUpdater())
      .join(vertexLabels)
      .where(0, 2).equalTo(0, 1) // graphId,targetId = graphId,vertexId
      .with(new TargetLabelUpdater());

    // 5. extend vertex labels by outgoing vertex+edge labels

    DataSet<VertexLabel> outgoingAdjacencyListLabels = edgeLabels
      .groupBy(0, 1) // graphId, sourceId
      .reduceGroup(new OutgoingAdjacencyListLabel());


    // 6. extend vertex labels by outgoing vertex+edge labels

    DataSet<VertexLabel> incomingAdjacencyListLabels = edgeLabels
      .groupBy(0, 2) // graphId, targetId
      .reduceGroup(new IncomingAdjacencyListLabel());

    // 7. combine vertex labels

    vertexLabels = vertexLabels
      .leftOuterJoin(outgoingAdjacencyListLabels)
      .where(0, 1).equalTo(0, 1)
      .with(new LabelAppender<VertexLabel>())
      .leftOuterJoin(incomingAdjacencyListLabels)
      .where(0, 1).equalTo(0, 1)
      .with(new LabelAppender<VertexLabel>());

    // 8. create adjacency matrix labels

    DataSet<GraphHeadLabel> adjacencyMatrixLabels = vertexLabels
      .groupBy(0)
      .reduceGroup(new AdjacencyMatrixLabel());

    // 9. combine graph labels

    graphHeadLabels = graphHeadLabels
      .join(adjacencyMatrixLabels)
      .where(0).equalTo(0)
      .with(new LabelAppender<GraphHeadLabel>());

    // 10. add empty head to prevent empty result for empty collection

    graphHeadLabels = graphHeadLabels
      .union(collection
        .getConfig()
        .getExecutionEnvironment()
        .fromElements(new GraphHeadLabel(GradoopId.get(), ""))
      );

    // 11. label collection

    return graphHeadLabels
      .reduceGroup(new CollectionLabel());
  }
}
