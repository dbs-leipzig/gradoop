package org.gradoop.flink.model.impl.operators.tostring.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMLabeled;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * (graphId, label)
 */
public class GraphHeadString extends Tuple2<GradoopId, String>
  implements EPGMLabeled {

  /**
   * default constructor
   */
  public GraphHeadString() {
  }

  /**
   * constructor with field values
   * @param id graph id
   * @param label graph head label
   */
  public GraphHeadString(GradoopId id, String label) {
    this.f0 = id;
    this.f1 = label;
  }

  @Override
  public String getLabel() {
    return this.f1;
  }

  @Override
  public void setLabel(String label) {
    this.f1 = label;
  }

  public GradoopId getId() {
    return this.f0;
  }
}
