
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;

/**
 * Maps a {@link VertexGroupItem} to a {@link VertexWithSuperVertex}.
 */
@FunctionAnnotation.ForwardedFields(
  "f0;" + // vertex id
  "f1"    // super vertex id
)
public class BuildVertexWithSuperVertex
  implements MapFunction<VertexGroupItem, VertexWithSuperVertex> {

  /**
   * Avoid object instantiation.
   */
  private final VertexWithSuperVertex reuseTuple;

  /**
   * Creates mapper.
   */
  public BuildVertexWithSuperVertex() {
    this.reuseTuple = new VertexWithSuperVertex();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexWithSuperVertex map(VertexGroupItem vertexGroupItem) throws
    Exception {
    reuseTuple.setVertexId(vertexGroupItem.getVertexId());
    reuseTuple.setSuperVertexId(vertexGroupItem.getSuperVertexId());
    return reuseTuple;
  }
}
