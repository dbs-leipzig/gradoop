
package org.gradoop.flink.algorithms.labelpropagation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Maps EPGM vertex to a Gelly vertex consisting of the EPGM identifier and the
 * label propagation value.
 */
@FunctionAnnotation.ForwardedFields("id->f0")
@FunctionAnnotation.ReadFields("properties")
public class VertexToGellyVertexMapper implements
  MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, PropertyValue>> {
  /**
   * Property key to access the label value which will be propagated
   */
  private final String propertyKey;

  /**
   * Reduce object instantiations
   */
  private final org.apache.flink.graph.Vertex<GradoopId, PropertyValue>
  reuseVertex;

  /**
   * Constructor
   *
   * @param propertyKey property key for get property value
   */
  public VertexToGellyVertexMapper(String propertyKey) {
    this.propertyKey = propertyKey;
    this.reuseVertex = new org.apache.flink.graph.Vertex<>();
  }

  @Override
  public org.apache.flink.graph.Vertex<GradoopId, PropertyValue> map(
    Vertex epgmVertex) throws Exception {
    reuseVertex.setId(epgmVertex.getId());
    reuseVertex.setValue(epgmVertex.getPropertyValue(propertyKey));
    return reuseVertex;
  }
}
