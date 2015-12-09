package org.gradoop.model.impl.algorithms.labelpropagation.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Updates the vertex on the left side with the property value on the right side
 *
 * @param <V> EPGM vertex type
 */
public class LPVertexJoin<V extends EPGMVertex> implements
  JoinFunction<Vertex<GradoopId, PropertyValue>, V, V> {
  /**
   * Property key to access the label value which will be propagated
   */
  private String propertyKey;

  /**
   * Constructor
   *
   * @param propertyKey Property key to access the label value
   */
  public LPVertexJoin(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public V join(Vertex<GradoopId, PropertyValue> gellyVertex, V epgmVertex)
      throws Exception {
    epgmVertex.setProperty(propertyKey, gellyVertex.getValue());
    return epgmVertex;
  }
}
