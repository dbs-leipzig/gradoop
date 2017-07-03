
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

/**
 * Takes a projected edge and an (vertex-id, group-representative) tuple
 * and replaces the edge-target-id with the group-representative.
 */
public class UpdateEdgeGroupItem
  implements JoinFunction<EdgeGroupItem, VertexWithSuperVertex, EdgeGroupItem> {
  /**
   * Field in {@link EdgeGroupItem} which is overridden by the group
   * representative id.
   */
  private final int field;
  /**
   * Creates new join function.
   *
   * @param field field that is overridden by the group representative
   */
  public UpdateEdgeGroupItem(int field) {
    this.field = field;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EdgeGroupItem join(EdgeGroupItem edge, VertexWithSuperVertex idTuple) throws Exception {
    edge.setField(idTuple.getSuperVertexId(), field);
    return edge;
  }
}
