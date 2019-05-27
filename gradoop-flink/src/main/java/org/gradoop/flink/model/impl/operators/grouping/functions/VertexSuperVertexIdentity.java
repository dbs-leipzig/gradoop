package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

/**
 * Creates a {@link VertexWithSuperVertex} with both components referencing the same
 * vertex that is mapped on.
 */
public class VertexSuperVertexIdentity<V extends EPGMVertex> implements
  MapFunction<V, VertexWithSuperVertex> {

  /**
   * Avoid object instantiation.
   */
  private final VertexWithSuperVertex reuseTuple;

  public VertexSuperVertexIdentity() {
    reuseTuple = new VertexWithSuperVertex();
  }

  @Override
  public VertexWithSuperVertex map(V value) throws Exception {
    reuseTuple.setVertexId(value.getId());
    reuseTuple.setSuperVertexId((value.getId()));
    return reuseTuple;
  }

}
