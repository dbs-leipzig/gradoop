/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.representation;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.api.pojos.AdjacencyListCellValueFactory;
import org.gradoop.flink.representation.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transaction.GraphTransaction;

import java.util.Map;
import java.util.Set;

/**
 * Util to convert among different graph representations.
 */
public class RepresentationConverters {

  /**
   * transaction => adjacency list
   *
   * @param transaction transaction
   * @param cellValueFactory factory for algorithm-specific cell values
   * @param <T> cell value type
   * @return adjacency list
   */
  public static <T> AdjacencyList<T> getAdjacencyList(
    GraphTransaction transaction, AdjacencyListCellValueFactory<T> cellValueFactory) {

    GraphHead graphHead = transaction.getGraphHead();
    Set<Vertex> vertices = transaction.getVertices();
    Set<Edge> edges = transaction.getEdges();

    Map<GradoopId, String> labels =
      Maps.newHashMapWithExpectedSize(1 + vertices.size() + edges.size());

    Map<GradoopId, Properties> properties =
      Maps.newHashMap();

    Map<GradoopId, AdjacencyListRow<T>> rows =
      Maps.newHashMapWithExpectedSize(vertices.size());

    Map<GradoopId, Vertex> vertexIndex =
      Maps.newHashMapWithExpectedSize(vertices.size());

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

  /**
   * Adds label and properties of an EPGM element to id-label and id-properties maps.
   *
   * @param element EPGM element
   * @param labels id-label map
   * @param properties id-properties map
   */
  private static void addLabelsAndProperties(
    EPGMElement element,
    Map<GradoopId, String> labels,
    Map<GradoopId, Properties> properties
  ) {
    labels.put(element.getId(), element.getLabel());

    Properties propertyList = element.getProperties();
    if (propertyList != null && !propertyList.isEmpty()) {
      properties.put(element.getId(), propertyList);
    }
  }

  /**
   * adjacency list => transaction
   *
   * @param adjacencyList adjacency list
   * @param <T> cell value type
   * @return transaction
   */
  public static <T> GraphTransaction getGraphTransaction(AdjacencyList<T>  adjacencyList) {

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
      Properties properties = adjacencyList.getProperties(sourceId);
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
