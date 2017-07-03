
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Update Function of vertex centric iteration.
 */
public class BtgUpdater
  extends GatherFunction<GradoopId, GradoopId, GradoopId> {

  @Override
  public void updateVertex(Vertex<GradoopId, GradoopId> vertex,
    MessageIterator<GradoopId> messageIterator) throws Exception {

    GradoopId lastComponent = vertex.getValue();
    GradoopId newComponent = lastComponent;

    for (GradoopId messageComponent : messageIterator) {

      if (messageComponent.compareTo(newComponent) < 0) {
        newComponent = messageComponent;
      }
    }

    if (!lastComponent.equals(newComponent)) {
      setNewVertexValue(newComponent);
    }
  }
}
