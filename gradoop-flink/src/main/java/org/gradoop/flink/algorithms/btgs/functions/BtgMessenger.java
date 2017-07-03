package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Update Function of vertex centric iteration.
 */
public class BtgMessenger
  extends ScatterFunction<GradoopId, GradoopId, GradoopId, NullValue> {

  @Override
  public void sendMessages(
    Vertex<GradoopId, GradoopId> vertex) throws Exception {

    sendMessageToAllNeighbors(vertex.getValue());
  }
}
