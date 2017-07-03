
package org.gradoop.flink.algorithms.labelpropagation.functions;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Distributes the new vertex value
 */
public class LPMessageFunction
  extends ScatterFunction<GradoopId, PropertyValue, PropertyValue, NullValue> {

  @Override
  public void sendMessages(Vertex<GradoopId, PropertyValue> vertex) throws
    Exception {
    sendMessageToAllNeighbors(vertex.getValue());
  }
}
