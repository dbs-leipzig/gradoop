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
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Map;
import java.util.Set;

/**
 * (g, V, E) => adjacencyList
 * for directed graph
 */
public class ToDirectedAdjacencyList implements
  MapFunction<GraphTransaction, AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel>> {

  @Override
  public AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> map(
    GraphTransaction transaction) throws Exception {

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
