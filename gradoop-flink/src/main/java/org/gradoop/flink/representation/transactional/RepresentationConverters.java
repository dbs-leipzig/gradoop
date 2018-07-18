/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
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

    Set<Vertex> vertices = transaction.getVertices();
    Set<Edge> edges = transaction.getEdges();

    int vertexCount = vertices.size();

    Map<GradoopId, String> labels =
      Maps.newHashMapWithExpectedSize(1 + vertexCount + edges.size());

    Map<GradoopId, Properties> properties =
      Maps.newHashMap();

    Map<GradoopId, AdjacencyListRow<ED, VD>> outgoingRows =
      Maps.newHashMapWithExpectedSize(vertexCount);

    Map<GradoopId, AdjacencyListRow<ED, VD>> incomingRows =
      Maps.newHashMapWithExpectedSize(vertexCount);

    Map<GradoopId, Vertex> vertexIndex = Maps.newHashMapWithExpectedSize(vertexCount);

    // VERTICES
    for (Vertex vertex : vertices) {
      addLabelsAndProperties(vertex, labels, properties);
      vertexIndex.put(vertex.getId(), vertex);
    }

    // EDGES

    for (Edge edge : edges) {
      addLabelsAndProperties(edge, labels, properties);

      Vertex source = vertexIndex.get(edge.getSourceId());
      AdjacencyListRow<ED, VD> outgoingRow =
        outgoingRows.computeIfAbsent(source.getId(), k -> new AdjacencyListRow<>());

      VD sourceData = vertexDataFactory.map(source);

      Vertex target = vertexIndex.get(edge.getTargetId());
      AdjacencyListRow<ED, VD> incomingRow =
        incomingRows.computeIfAbsent(target.getId(), k -> new AdjacencyListRow<>());
      VD targetData = vertexDataFactory.map(target);

      ED edgeData = edgeDataFactory.map(edge);

      outgoingRow.getCells().add(new AdjacencyListCell<>(edgeData, targetData));
      incomingRow.getCells().add(new AdjacencyListCell<>(edgeData, sourceData));
    }

    return new AdjacencyList<>(
      transaction.getGraphHead(), labels, properties, outgoingRows, incomingRows);
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
