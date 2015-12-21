package org.gradoop.model.impl.operators.cam.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMLabeled;
import org.gradoop.model.impl.id.GradoopId;

/**
 * (graphId, vertexId, label)
 */
public class VertexLabel extends Tuple3<GradoopId, GradoopId, String>
  implements EPGMLabeled {

  public VertexLabel() {

  }

  public VertexLabel(GradoopId graphId, GradoopId vertexId, String label) {
    this.f0 = graphId;
    this.f1 = vertexId;
    this.f2 = label;
  }

  public String getLabel() {
    return this.f2;
  }

  public void setLabel(String label) {
    this.f2 = label;
  }

  public GradoopId getGraphId() {
    return this.f0;
  }
}
