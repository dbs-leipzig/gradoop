package org.gradoop.flink.datagen.foodbroker.complainthandling;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;

import java.util.Set;

/**
 * Created by Stephan on 15.08.16.
 */
public class Client extends MasterDataFromEdge {

  public Client(VertexFactory vertexFactory) {
    super(vertexFactory);
  }

  @Override
  public void flatMap( Set<Edge> edges,
    Collector<Tuple2<String, Vertex>> collector) throws Exception {
    Vertex customer;
    PropertyList properties;
    for (Edge edge : edges) {
      if (edge.getLabel().equals("openedBy")) {
        customer = getVertex(edge.getTargetId());
        properties = customer.getProperties();
        properties.set("erpCustNum", customer.getId().toString());
        properties.set("contactPhone", "0123456789");
        properties.set("account","CL" + customer.getId().toString());
        Vertex client = vertexFactory
          .createVertex("User", properties, edge.getGraphIds());
        collector.collect(
          new Tuple2<>(customer.getId().toString(), client));
      }
    }
  }
}
