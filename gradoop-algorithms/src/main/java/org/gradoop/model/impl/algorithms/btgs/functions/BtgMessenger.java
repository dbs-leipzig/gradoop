package org.gradoop.model.impl.algorithms.btgs.functions;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.types.NullValue;
import org.gradoop.model.impl.id.GradoopId;

public class BtgMessenger
  extends MessagingFunction
  <GradoopId, GradoopId, GradoopId, NullValue> {

  @Override
  public void sendMessages(Vertex<GradoopId, GradoopId> vertex)
    throws Exception {

    sendMessageToAllNeighbors(vertex.getValue());
  }
}
