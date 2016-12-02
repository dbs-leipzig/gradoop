package org.gradoop.flink.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Sets;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Set;


public class RelevantElementsFromBrokerage
  implements FlatMapFunction<GraphTransaction, GraphTransaction> {
  @Override
  public void flatMap(GraphTransaction graphTransaction,
    Collector<GraphTransaction> collector) throws Exception {
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
    if (!vertices.isEmpty()) {
      graphTransaction.setVertices(vertices);
      graphTransaction.setEdges(edges);
      collector.collect(graphTransaction);
    }
  }
}