package org.gradoop.flink.model.impl.operators.fusion.reduce.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Tuple representing the fused vertex, and eventually its id (boolean)
 * in the graph operand
 *
 * f0: fused vertex
 * f1: if f2 appears in the graph operand
 * f2: operand's vertex id matching with f0.
 *
 * Created by Giacomo Bergami on 16/02/17.
 */
public class DisambiguationTupleWithVertexId extends Tuple3<Vertex, Boolean, GradoopId> {
  /**
   * Default constructor
   */
  public DisambiguationTupleWithVertexId() {
    super();
  }

}