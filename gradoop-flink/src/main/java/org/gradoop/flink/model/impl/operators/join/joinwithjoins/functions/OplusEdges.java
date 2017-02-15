package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.functions.Function;

import java.io.Serializable;

/**
 * Defining the way to combine the edges
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
public class OplusEdges extends OplusSemiConcrete<Edge> implements Serializable {

  /**
   * @{
   * @param transformation
   */
  public OplusEdges(Function<Tuple2<String, String>, String> transformation) {
    super(transformation);
  }

  @Override
  public Edge supplyEmpty() {
    Edge e = new Edge();
    e.setId(GradoopId.get());
    return e;
  }

}
