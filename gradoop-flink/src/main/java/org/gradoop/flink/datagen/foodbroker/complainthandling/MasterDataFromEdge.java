package org.gradoop.flink.datagen.foodbroker.complainthandling;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;

import java.util.List;
import java.util.Set;

/**
 * Created by Stephan on 15.08.16.
 */
public abstract class MasterDataFromEdge extends
  RichFlatMapFunction<Set<Edge>, Tuple2<String, Vertex>> {
  protected VertexFactory vertexFactory;

  List<Vertex> vertices;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    vertices = getRuntimeContext().getBroadcastVariable("empCust");
  }

  public MasterDataFromEdge(VertexFactory vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  protected Vertex getVertex(GradoopId id) {
    for (Vertex vertex : vertices) {
      if (vertex.getId().equals(id)) {
        return vertex;
      }
    }
    return null;
  }
}
