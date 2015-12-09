package org.gradoop.model.impl.algorithms.labelpropagation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Maps EPGM vertex to a Gelly vertex consisting of the EPGM identifier and the
 * label propagation value.
 *
 * @param <V> EPGM vertex type
 */
public class VertexToGellyVertexMapper<V extends EPGMVertex>
  implements MapFunction<V, Vertex<GradoopId, PropertyValue>> {
  /**
   * Property key to access the label value which will be propagated
   */
  private final String propertyKey;

  /**
   * Reduce object instantiations
   */
  private final Vertex<GradoopId, PropertyValue> reuseVertex;

  /**
   * Constructor
   *
   * @param propertyKey property key for get property value
   */
  public VertexToGellyVertexMapper(String propertyKey) {
    this.propertyKey = propertyKey;
    this.reuseVertex = new Vertex<>();
  }

  @Override
  public Vertex<GradoopId, PropertyValue> map(V epgmVertex) throws Exception {
    reuseVertex.setId(epgmVertex.getId());
    reuseVertex.setValue(epgmVertex.getPropertyValue(propertyKey));
    return reuseVertex;
  }
}
