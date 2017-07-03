
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;

/**
 * Combines a group of {@link VertexGroupItem} instances.
 *
 * Note that this needs to be a subclass of {@link ReduceVertexGroupItems}. If
 * the base class would implement {@link GroupCombineFunction} and
 * {@link GroupReduceFunction}, a groupReduce call would automatically call
 * the combiner (which is not wanted).
 */
@FunctionAnnotation.ForwardedFields(
    "f0;" + // vertexId
    "f3;" + // label
    "f4;"  + // properties
    "f6"    // label group
)
public class CombineVertexGroupItems
  extends ReduceVertexGroupItems
  implements GroupCombineFunction<VertexGroupItem, VertexGroupItem> {

  /**
   * Creates group reduce function.
   *
   * @param useLabel                        true, iff labels are used for grouping
   */
  public CombineVertexGroupItems(boolean useLabel) {
    super(useLabel);
  }

  @Override
  public void combine(Iterable<VertexGroupItem> values,
    Collector<VertexGroupItem> out) throws Exception {
    reduce(values, out);
  }
}
