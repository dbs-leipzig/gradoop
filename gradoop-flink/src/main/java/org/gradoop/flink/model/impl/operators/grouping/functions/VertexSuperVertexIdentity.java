package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

/**
 * Creates a {@link VertexWithSuperVertex} with both components referencing the same
 * vertex that is mapped on.
 */
public class VertexSuperVertexIdentity implements
  MapFunction<Vertex, VertexWithSuperVertex> {

  /**
   * Avoid object instantiation.
   */
  private final VertexWithSuperVertex reuseTuple;

  public VertexSuperVertexIdentity() {
    reuseTuple = new VertexWithSuperVertex();
  }

  @Override
  public VertexWithSuperVertex map(Vertex value) throws Exception {
    reuseTuple.setVertexId(value.getId());
    reuseTuple.setSuperVertexId((value.getId()));
    return reuseTuple;
  }

}
