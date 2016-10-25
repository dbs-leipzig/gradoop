package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;


public class VertexToVertexIds implements
  MapFunction<Vertex, Tuple2<String, GradoopId>> {

  @Override
  public Tuple2<String, GradoopId> map(Vertex vertex) throws Exception {
    return new Tuple2<String, GradoopId>(
      vertex.getPropertyValue(
        CSVToElement.PROPERTY_KEY_KEY).getString(), vertex.getId());
  }
}
