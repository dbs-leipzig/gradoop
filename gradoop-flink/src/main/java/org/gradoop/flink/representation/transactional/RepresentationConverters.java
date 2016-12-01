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

package org.gradoop.flink.representation.transactional;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListRow;

import java.util.Map;
import java.util.Set;

/**
 * Util to convert among different graph representations.
 */
public class RepresentationConverters {

  /**
   * transaction => adjacency list
   *
   * @param transaction (g,V,E)
   *
   * @param edgeDataFactory edge data factory
   * @param vertexDataFactory vertex data factory
   *
   * @param <ED> edge data
   * @param <VD> vertex data
   *
   * @return adjacency list
   */
  public static <ED, VD> AdjacencyList<GradoopId, String, ED, VD> getAdjacencyList(
    GraphTransaction transaction,
    MapFunction<Edge, ED> edgeDataFactory,
    MapFunction<Vertex, VD> vertexDataFactory
  ) throws Exception {

    GraphHead graphHead = transaction.getGraphHead();
    Set<Vertex> vertices = transaction.getVertices();
    Set<Edge> edges = transaction.getEdges();

    Map<GradoopId, String> labels =
      Maps.newHashMapWithExpectedSize(1 + vertices.size() + edges.size());

    Map<GradoopId, Properties> properties =
      Maps.newHashMap();

    Map<GradoopId, AdjacencyListRow<ED, VD>> outgoingRows =
      Maps.newHashMapWithExpectedSize(vertices.size());

    Map<GradoopId, AdjacencyListRow<ED, VD>> incomingRows =
      Maps.newHashMapWithExpectedSize(vertices.size());

    Map<GradoopId, Vertex> vertexIndex =
      Maps.newHashMapWithExpectedSize(vertices.size());

    // VERTICES
    for (Vertex vertex : vertices) {
      addLabelsAndProperties(vertex, labels, properties);
      vertexIndex.put(vertex.getId(), vertex);
      outgoingRows.put(vertex.getId(), new AdjacencyListRow<>());
      incomingRows.put(vertex.getId(), new AdjacencyListRow<>());
    }

    // EDGES
    for (Edge edge : edges) {
      addLabelsAndProperties(edge, labels, properties);

      Vertex source = vertexIndex.get(edge.getSourceId());
      AdjacencyListRow<ED, VD> sourceRows = outgoingRows.get(source.getId());

      Vertex target = vertexIndex.get(edge.getTargetId());
      AdjacencyListRow<ED, VD> targetRows = incomingRows.get(target.getId());

      sourceRows.getCells().add(new AdjacencyListCell<>(
        edgeDataFactory.map(edge), vertexDataFactory.map(target)));

      targetRows.getCells().add(new AdjacencyListCell<>(
        edgeDataFactory.map(edge), vertexDataFactory.map(source)));
    }

    return new AdjacencyList<>(graphHead, labels, properties, outgoingRows, incomingRows);
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
   * @return transaction
   */
  public static GraphTransaction getGraphTransaction(AdjacencyList<GradoopId, String, GradoopId,
    GradoopId> adjacencyList) {

    // GRAPH HEAD
    GraphHead graphHead = adjacencyList.getGraphHead();

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());

    Set<Vertex> vertices = Sets.newHashSet();
    Set<Edge> edges = Sets.newHashSet();

    // VERTICES
    for (Map.Entry<GradoopId, AdjacencyListRow<GradoopId, GradoopId>> entry :
      adjacencyList.getOutgoingRows().entrySet()) {

      GradoopId sourceId = entry.getKey();
      Properties properties = adjacencyList.getProperties(sourceId);
      String label = adjacencyList.getLabel(sourceId);
      vertices.add(new Vertex(sourceId, label, properties, graphIds));

      // EDGES
      for (AdjacencyListCell<GradoopId, GradoopId> cell : entry.getValue().getCells()) {
        GradoopId edgeId = cell.getEdgeData();
        label = adjacencyList.getLabel(edgeId);
        properties = adjacencyList.getProperties(edgeId);
        GradoopId targetId = cell.getVertexData();

        edges.add(new Edge(edgeId, label, sourceId, targetId, properties, graphIds));
      }
    }

    return new GraphTransaction(graphHead, vertices, edges);
  }

}
