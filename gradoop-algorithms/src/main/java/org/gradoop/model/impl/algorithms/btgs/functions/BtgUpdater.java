package org.gradoop.model.impl.algorithms.btgs.functions;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.gradoop.model.impl.id.GradoopId;

public class BtgUpdater extends VertexUpdateFunction
  <GradoopId, GradoopId, GradoopId> {

  @Override
  public void updateVertex(Vertex<GradoopId, GradoopId> vertex,
    MessageIterator<GradoopId> messageIterator) throws Exception {

    GradoopId lastComponent = vertex.getValue();
    GradoopId newComponent = lastComponent;

    for(GradoopId messageComponent : messageIterator) {

      if(messageComponent.compareTo(newComponent) < 0) {
        newComponent = messageComponent;
      }
    }

    if(!lastComponent.equals(newComponent)) {
      setNewVertexValue(newComponent);
    }
  }
}
