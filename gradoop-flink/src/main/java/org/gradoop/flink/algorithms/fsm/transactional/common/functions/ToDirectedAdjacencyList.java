package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.functions.tuple.ToIdWithLabel;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transactional.AdjacencyList;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.representation.transactional.RepresentationConverters;

import java.util.Map;
import java.util.Set;


public class ToDirectedAdjacencyList
  implements MapFunction<GraphTransaction, AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel>> {

  @Override
  public AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> map(GraphTransaction transaction) throws Exception {
    Set<Vertex> vertices = transaction.getVertices();
    Set<Edge> edges = transaction.getEdges();

    int vertexCount = vertices.size();

    Map<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> outgoingRows =
      Maps.newHashMapWithExpectedSize(vertexCount);

    Map<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> incomingRows =
      Maps.newHashMapWithExpectedSize(vertexCount);

    Map<GradoopId, String> labels = Maps.newHashMapWithExpectedSize(vertexCount);

    // VERTICES
    for (Vertex vertex : vertices) {
      labels.put(vertex.getId(), vertex.getLabel());
    }

    // EDGES

    for (Edge edge : edges) {
      GradoopId sourceId = edge.getSourceId();

      AdjacencyListRow<IdWithLabel, IdWithLabel> outgoingRow =
        outgoingRows.computeIfAbsent(sourceId, k -> new AdjacencyListRow<>());

      IdWithLabel sourceData = new IdWithLabel(sourceId, labels.get(sourceId));

      GradoopId targetId = edge.getTargetId();
      AdjacencyListRow<IdWithLabel, IdWithLabel> incomingRow =
        incomingRows.computeIfAbsent(targetId, k -> new AdjacencyListRow<>());

      IdWithLabel targetData = new IdWithLabel(targetId, labels.get(targetId));

      IdWithLabel edgeData = new IdWithLabel(edge.getId(), edge.getLabel());

      outgoingRow.getCells().add(new AdjacencyListCell<>(edgeData, targetData));
      incomingRow.getCells().add(new AdjacencyListCell<>(edgeData, sourceData));
    }

    return new AdjacencyList<>(
      transaction.getGraphHead(), labels, null, outgoingRows, incomingRows);
  }
}
