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
package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transactional.AdjacencyList;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.Map;
import java.util.Set;

/**
 * (g, V, E) => adjacencyList
 * for undirected graph
 */
public class ToUndirectedAdjacencyList implements
  MapFunction<GraphTransaction, AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel>> {

  @Override
  public AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> map(
    GraphTransaction transaction) throws Exception {

    Set<Vertex> vertices = transaction.getVertices();
    Set<Edge> edges = transaction.getEdges();

    int vertexCount = vertices.size();

    Map<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> rows =
      Maps.newHashMapWithExpectedSize(vertexCount);

    Map<GradoopId, String> labels = Maps.newHashMapWithExpectedSize(vertexCount);

    // VERTICES
    for (Vertex vertex : vertices) {
      labels.put(vertex.getId(), vertex.getLabel());
    }

    // EDGES

    for (Edge edge : edges) {
      GradoopId sourceId = edge.getSourceId();
      GradoopId targetId = edge.getTargetId();

      AdjacencyListRow<IdWithLabel, IdWithLabel> sourceRow =
        rows.computeIfAbsent(sourceId, k -> new AdjacencyListRow<>());

      IdWithLabel edgeData = new IdWithLabel(edge.getId(), edge.getLabel());
      IdWithLabel targetData = new IdWithLabel(targetId, labels.get(targetId));

      sourceRow.getCells().add(new AdjacencyListCell<>(edgeData, targetData));

      if (! sourceId.equals(targetId)) {
        IdWithLabel sourceData = new IdWithLabel(sourceId, labels.get(sourceId));

        AdjacencyListRow<IdWithLabel, IdWithLabel> targetRow =
          rows.computeIfAbsent(targetId, k -> new AdjacencyListRow<>());

        targetRow.getCells().add(new AdjacencyListCell<>(edgeData, sourceData));
      }
    }

    return new AdjacencyList<>(
      transaction.getGraphHead(), labels, null, rows, Maps.newHashMap());
  }
}
