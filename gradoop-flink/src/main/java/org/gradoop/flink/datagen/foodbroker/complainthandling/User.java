package org.gradoop.flink.datagen.foodbroker.complainthandling;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyList;

import java.util.Set;

/**
 * Created by Stephan on 15.08.16.
 */
public class User extends MasterDataFromEdge {


  public User(VertexFactory vertexFactory) {
    super(vertexFactory);
  }

  @Override
  public void flatMap(
    Set<Edge> edges,
    Collector<Tuple2<String, Vertex>> collector) throws Exception {
    Vertex employee;
    PropertyList properties;
    for (Edge edge : edges) {
      if (edge.getLabel().equals("createdBy")
        || edge.getLabel().equals("allocatedTo")) {
        properties = new PropertyList();
        employee = getVertex(edge.getTargetId());
        properties = employee.getProperties();

        properties.set("erpEmplNum", employee.getId().toString());
        String email = properties.get("name").getString();
        email = email.replace(" ",".").toLowerCase();
        email += "@biiig.org";
        properties.set("email", email);
        properties.remove("num");
        properties.remove("sid");
        Vertex user = vertexFactory
          .createVertex("User", properties, edge.getGraphIds());

        System.out.println("user: " + user.toString());
//        System.exit(0);
        Tuple2<String, Vertex> tuple =
          new Tuple2<String, Vertex>(employee.getId().toString(), user);
//          new Tuple2<String, Vertex>(employee.getId().toString(), vertexFactory.createVertex());

        collector.collect(tuple);
      }
    }
  }
}
