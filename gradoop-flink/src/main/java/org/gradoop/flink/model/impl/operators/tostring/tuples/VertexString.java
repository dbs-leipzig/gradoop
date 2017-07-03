package org.gradoop.flink.model.impl.operators.tostring.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.EPGMLabeled;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * (graphId, vertexId, label)
 */
public class VertexString extends Tuple3<GradoopId, GradoopId, String>
  implements EPGMLabeled {

  /**
   * default constructor
   */
  public VertexString() {
  }

  /**
   * constructor with field values
   * @param graphId graph id
   * @param id vertex id
   * @param label vertex label
   */
  public VertexString(GradoopId graphId, GradoopId id, String label) {
    this.f0 = graphId;
    this.f1 = id;
    this.f2 = label;
  }

  public GradoopId getGraphId() {
    return this.f0;
  }

  @Override
  public String getLabel() {
    return this.f2;
  }

  @Override
  public void setLabel(String label) {
    this.f2 = label;
  }
}
