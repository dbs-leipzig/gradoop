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

package org.gradoop.flink.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Sets;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Set;

/**
 * Filters all vertices and edges from the graph transaction which are relevant for the complaint
 * handling process.
 */
public class RelevantElementsFromBrokerage
  implements FlatMapFunction<GraphTransaction, GraphTransaction> {
  @Override
  public void flatMap(GraphTransaction graphTransaction,
    Collector<GraphTransaction> collector) throws Exception {
    //take only vertices which have the needed label
    Set<Vertex> vertices = Sets.newHashSet();
    for (Vertex vertex : graphTransaction.getVertices()) {
      if (vertex.getLabel().equals("DeliveryNote") || vertex.getLabel().equals("SalesOrder")) {
        vertices.add(vertex);
      }
    }
    //take only edges which have the needed label
    Set<String> edgelabels = Sets.newHashSet("contains", "receives", "operatedBy", "placedAt",
      "receivedFrom", "SalesOrderLine", "PurchOrderLine");
    Set<Edge> edges = Sets.newHashSet();
    for (Edge edge : graphTransaction.getEdges()) {
      if (edgelabels.contains(edge.getLabel())) {
        edges.add(edge);
      }
    }
    //vertices is empty if the sales quotation has not been confirmed during the brokerage process
    if (!vertices.isEmpty()) {
      //store only relevant information in the graph transaction
      graphTransaction.setVertices(vertices);
      graphTransaction.setEdges(edges);
      collector.collect(graphTransaction);
    }
  }
}