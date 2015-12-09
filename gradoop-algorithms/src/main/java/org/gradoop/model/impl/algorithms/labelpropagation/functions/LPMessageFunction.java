package org.gradoop.model.impl.algorithms.labelpropagation.functions;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.types.NullValue;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Distributes the new vertex value
 */
public class LPMessageFunction
  extends MessagingFunction
  <GradoopId, PropertyValue, PropertyValue, NullValue> {

  @Override
  public void sendMessages(Vertex<GradoopId, PropertyValue> vertex) throws
    Exception {
    sendMessageToAllNeighbors(vertex.getValue());
  }
}
