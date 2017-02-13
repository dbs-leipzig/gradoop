package org.gradoop.flink.model.impl.operators.join.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;

import java.io.Serializable;

/**
 * Created by vasistas on 01/02/17.
 */
public class OplusVertex extends OplusSemiConcrete<Vertex> implements Serializable {

  public OplusVertex(Function<Tuple2<String, String>, String> transformation) {
    super(transformation);
  }

  @Override
  public Vertex supplyEmpty() {
    Vertex v = new Vertex();
    v.setId(GradoopId.get());
    return v;
  }

}
