
package org.gradoop.flink.algorithms.labelpropagation.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Updates the vertex on the left side with the property value on the right side
 */
@FunctionAnnotation.ForwardedFieldsSecond("id;label;graphIds")
@FunctionAnnotation.ReadFieldsFirst("f1")
public class LPVertexJoin implements JoinFunction
  <org.apache.flink.graph.Vertex<GradoopId, PropertyValue>, Vertex, Vertex> {

  /**
   * Property key to access the value which will be propagated
   */
  private final String propertyKey;

  /**
   * Constructor
   *
   * @param propertyKey property key to access the propagation value
   */
  public LPVertexJoin(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public Vertex join(
    org.apache.flink.graph.Vertex<GradoopId, PropertyValue> gellyVertex,
    Vertex epgmVertex) throws Exception {
    epgmVertex.setProperty(propertyKey, gellyVertex.getValue());
    return epgmVertex;
  }
}
