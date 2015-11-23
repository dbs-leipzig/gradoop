package org.gradoop.model.impl.operators.equality.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.api.EPGMLabeled;
import org.gradoop.model.impl.id.GradoopId;

public class EdgeDataLabel
  extends Tuple4<GradoopId, GradoopId, GradoopId, String>
  implements EPGMLabeled {

  public EdgeDataLabel(){
  }

  public EdgeDataLabel(
    GradoopId sourceVertexId, GradoopId targetVertexId, String label) {
    this.f0 = new GradoopId();
    this.f1 = sourceVertexId;
    this.f2 = targetVertexId;
    this.f3 = label;
  }

  public GradoopId getGraphId() {
    return this.f0;
  }

  public void setGraphId(GradoopId graphId) {
    this.f0 = graphId;
  }

  public GradoopId getSourceVertexId() {
    return this.f1;
  }

  public void setSourceVertexId(GradoopId id) {
    this.f1 = id;
  }

  public GradoopId getTargetVertexId() {
    return this.f2;
  }

  public void setTargetVertexId(GradoopId id) {
    this.f2 = id;
  }

  public String getLabel() {
    return this.f3;
  }

  public void setLabel(String label) {
    this.f3 = label;
  }

}
