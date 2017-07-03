package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * turns an EPGM edge into a Gelly edge without data.
 */
public class ToGellyEdgeWithNullValue implements
  MapFunction<Edge, org.apache.flink.graph.Edge<GradoopId, NullValue>> {

  /**
   * Reduce instantiations
   */
  private final org.apache.flink.graph.Edge<GradoopId, NullValue> reuse;

  /**
   * Constructor
   */
  public ToGellyEdgeWithNullValue() {
    reuse = new org.apache.flink.graph.Edge<>();
    reuse.f2 = NullValue.getInstance();
  }

  @Override
  public org.apache.flink.graph.Edge<GradoopId, NullValue> map(Edge e)
      throws Exception {
    reuse.setSource(e.getSourceId());
    reuse.setTarget(e.getTargetId());
    return reuse;
  }
}
