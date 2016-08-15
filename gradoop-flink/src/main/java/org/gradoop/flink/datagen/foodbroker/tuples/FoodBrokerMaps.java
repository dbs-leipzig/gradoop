package org.gradoop.flink.datagen.foodbroker.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Map;
import java.util.Set;

/**
 * Created by Stephan on 10.08.16.
 */
public class FoodBrokerMaps
  extends Tuple2<Map<GradoopId, Vertex>, Map<Tuple2<String, GradoopId>, Set<GradoopId>>> {


  public FoodBrokerMaps() {
  }

  public FoodBrokerMaps(Map<GradoopId, Vertex> vertexMap,
    Map<Tuple2<String, GradoopId>, Set<GradoopId>> edgeMap) {
    super(vertexMap, edgeMap);
  }

  public Map<GradoopId, Vertex> getVertexMap() {
    return f0;
  }

  public Map<Tuple2<String, GradoopId>, Set<GradoopId>> getEdgeMap() {
    return f1;
  }
}
