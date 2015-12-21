package org.gradoop.model.impl.operators.cam.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMLabeled;
import org.gradoop.model.impl.id.GradoopId;

/**
 * (graphId, label)
 */
public class GraphHeadLabel extends Tuple2<GradoopId, String>
  implements EPGMLabeled{

  public GraphHeadLabel() {

  }

  public GraphHeadLabel(GradoopId id, String label) {
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
}
