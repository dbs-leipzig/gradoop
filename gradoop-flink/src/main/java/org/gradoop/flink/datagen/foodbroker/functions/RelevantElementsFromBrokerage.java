package org.gradoop.flink.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Sets;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Set;


public class RelevantElementsFromBrokerage implements MapFunction<GraphTransaction, GraphTransaction> {
  @Override
  public GraphTransaction map(GraphTransaction graphTransaction) throws Exception {
    Set<Vertex> vertices = Sets.newHashSet();
    for (Vertex vertex : graphTransaction.getVertices()) {
      if (vertex.getLabel().equals("DeliveryNote") || vertex.getLabel().equals("SalesOrder")) {
        vertices.add(vertex);
      }
    }
    Set<String> edgelabels = Sets.newHashSet("contains", "receives", "operatedBy", "placedAt",
      "receivedFrom", "SalesOrderLine", "PurchOrderLine");
    Set<Edge> edges = Sets.newHashSet();
    for (Edge edge : graphTransaction.getEdges()) {
      if (edgelabels.contains(edge.getLabel())) {
        edges.add(edge);
      }
    }
    graphTransaction.setVertices(vertices);
    graphTransaction.setEdges(edges);
    return graphTransaction;
  }
}