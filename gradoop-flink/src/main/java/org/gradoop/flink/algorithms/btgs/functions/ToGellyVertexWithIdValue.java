
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * EPGM vertex to gelly vertex, where value is vertex id
 */
@FunctionAnnotation.ForwardedFields("id->f0;id->f1")
public class ToGellyVertexWithIdValue implements
  MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, GradoopId>> {

  /**
   * Reduce object instantiations
   */
  private final org.apache.flink.graph.Vertex<GradoopId, GradoopId> reuse =
    new org.apache.flink.graph.Vertex<>();

  @Override
  public org.apache.flink.graph.Vertex<GradoopId, GradoopId> map(Vertex vertex)
      throws Exception {
    GradoopId id = vertex.getId();
    reuse.setId(id);
    reuse.setValue(id);
    return reuse;
  }
}
