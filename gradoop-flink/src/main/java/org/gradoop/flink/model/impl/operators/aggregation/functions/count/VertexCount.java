
package org.gradoop.flink.model.impl.operators.aggregation.functions.count;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

/**
 * Aggregate function returning the vertex count of a graph / graph collection.
 */
public class VertexCount extends Count implements VertexAggregateFunction {

  @Override
  public PropertyValue getVertexIncrement(Vertex vertex) {
    return PropertyValue.create(1L);
  }

  @Override
  public String getAggregatePropertyKey() {
    return "vertexCount";
  }
}
