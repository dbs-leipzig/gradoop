package org.gradoop.flink.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.model.api.pojos.AdjacencyListCellValueFactory;
import org.gradoop.flink.model.impl.pojos.AdjacencyListCell;
import org.gradoop.flink.model.impl.pojos.AdjacencyListRow;
import org.gradoop.flink.model.impl.tuples.AdjacencyList;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Map;
import java.util.Set;

public class TransactionRepresentationConverter {

  public <T> AdjacencyList<T> getAdjacencyList(
    GraphTransaction transaction, AdjacencyListCellValueFactory<T> cellValueFactory) {

    GraphHead graphHead = transaction.getGraphHead();
    Set<Vertex> vertices = transaction.getVertices();
    Set<Edge> edges = transaction.getEdges();

    Map<GradoopId, String> labels =
      Maps.newHashMapWithExpectedSize(1 + vertices.size() + edges.size());

    Map<GradoopId, PropertyList> properties =
      Maps.newHashMap();

    Map<GradoopId, AdjacencyListRow<T>> rows =
      Maps.newHashMapWithExpectedSize(vertices.size());

    Map<GradoopId, Vertex> vertexIndex = Maps.newHashMapWithExpectedSize(vertices.size());

    // GRAPH HEAD
    addLabelsAndProperties(graphHead, labels, properties);

    // VERTICES
    for (Vertex vertex : vertices) {
      addLabelsAndProperties(vertex, labels, properties);
      vertexIndex.put(vertex.getId(), vertex);
      rows.put(vertex.getId(), new AdjacencyListRow<>());
    }

    // EDGES
    for (Edge edge : edges) {
      addLabelsAndProperties(edge, labels, properties);

      Vertex source = vertexIndex.get(edge.getSourceId());
      AdjacencyListRow<T> sourceRows = rows.get(source.getId());

      Vertex target = vertexIndex.get(edge.getTargetId());
      AdjacencyListRow<T> targetRows = rows.get(target.getId());

      sourceRows.getCells().add(new AdjacencyListCell<>(
        edge.getId(), true, target.getId(), cellValueFactory.createValue(source, edge, target)));

      targetRows.getCells().add(new AdjacencyListCell<>(
        edge.getId(), false, source.getId(), cellValueFactory.createValue(target, edge, source)));
    }

    return new AdjacencyList<>(graphHead.getId(), labels, properties, rows);
  }

  private void addLabelsAndProperties(EPGMElement graphHead, Map<GradoopId, String> labels,
    Map<GradoopId, PropertyList> properties) {
    labels.put(graphHead.getId(), graphHead.getLabel());

    PropertyList propertyList = graphHead.getProperties();
    if (propertyList != null && !propertyList.isEmpty()) {
      properties.put(graphHead.getId(), propertyList);
    }
  }

  public <T> GraphTransaction getGraphTransaction(AdjacencyList<T>  adjacencyList) {

    // GRAPH HEAD
    GradoopId graphId = adjacencyList.getGraphId();
    GraphHead graphHead =
      new GraphHead(graphId, adjacencyList.getLabel(graphId), adjacencyList.getProperties(graphId));

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphId);

    Set<Vertex> vertices = Sets.newHashSet();
    Set<Edge> edges = Sets.newHashSet();

    // VERTICES
    for (Map.Entry<GradoopId, AdjacencyListRow<T>> entry : adjacencyList.getRows().entrySet()) {
      GradoopId sourceId = entry.getKey();
      PropertyList properties = adjacencyList.getProperties(sourceId);
      String label = adjacencyList.getLabel(sourceId);
      vertices.add(new Vertex(sourceId, label, properties, graphIds));

      // EDGES
      for (AdjacencyListCell<T> cell : entry.getValue().getCells()) {

        if (cell.isOutgoing()) {
          GradoopId edgeId = cell.getEdgeId();
          label = adjacencyList.getLabel(edgeId);
          properties = adjacencyList.getProperties(edgeId);
          GradoopId targetId = cell.getVertexId();

          edges.add(new Edge(edgeId, label, sourceId, targetId, properties, graphIds));
        }
      }
    }

    return new GraphTransaction(graphHead, vertices, edges);
  }

}
