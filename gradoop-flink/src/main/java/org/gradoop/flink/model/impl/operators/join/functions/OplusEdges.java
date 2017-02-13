package org.gradoop.flink.model.impl.operators.join.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;

import java.io.Serializable;

/**
 * Created by vasistas on 01/02/17.
 */
public class OplusEdges extends OplusSemiConcrete<Edge> implements Serializable {

  public OplusEdges(Function<Tuple2<String, String>, String> transformation) {
    super(transformation);
  }

  @Override
  public Edge supplyEmpty() {
    Edge v = new Edge();
    v.setId(GradoopId.get());
    return v;
  }

}
