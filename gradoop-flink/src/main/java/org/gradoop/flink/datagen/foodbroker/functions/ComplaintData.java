package org.gradoop.flink.datagen.foodbroker.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.foodbroker.tuples.FoodBrokerMaps;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Set;

public class ComplaintData
  implements MapFunction<Tuple2<GraphTransaction, FoodBrokerMaps>,
  Tuple4<Set<Vertex>, FoodBrokerMaps, Set<Edge>, Set<Edge>>> {

  @Override
  public Tuple4<Set<Vertex>, FoodBrokerMaps, Set<Edge>, Set<Edge>> map(
    Tuple2<GraphTransaction, FoodBrokerMaps> tuple) throws
    Exception {
    Set<Vertex> deliveryNotes = Sets.newHashSet();
    for (Vertex vertex : tuple.f0.getVertices()) {
      if (vertex.getLabel().equals("DeliveryNote")){
        deliveryNotes.add(vertex);
      }
    }
    Set<Edge> salesOrderLines = Sets.newHashSet();
    Set<Edge> purchOrderLines = Sets.newHashSet();
    for (Edge edge : tuple.f0.getEdges()) {
      if (edge.getLabel().equals("SalesOrderLine")) {
        salesOrderLines.add(edge);
      } else if (edge.getLabel().equals("PurchOrderLine")) {
        purchOrderLines.add(edge);
      }
    }
    return new Tuple4<>(deliveryNotes, tuple.f1, salesOrderLines, purchOrderLines);
  }
}
