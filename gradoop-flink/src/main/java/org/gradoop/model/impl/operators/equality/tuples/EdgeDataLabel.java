package org.gradoop.model.impl.operators.equality.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMLabeled;
import org.gradoop.model.impl.id.GradoopId;

public class EdgeDataLabel extends Tuple3<GradoopId, GradoopId, String>
  implements EPGMLabeled {

  public EdgeDataLabel(){
  }

  public EdgeDataLabel(
    GradoopId sourceVertexId, GradoopId targetVertexId, String label) {

    this.f0 = sourceVertexId;
    this.f1 = targetVertexId;
    this.f2 = label;
  }

  public GradoopId getSourceVertexId() {
    return this.f0;
  }

  public void setSourceVertexId(GradoopId id) {
    this.f0 = id;
  }

  public GradoopId getTargetVertexId() {
    return this.f1;
  }

  public void setTargetVertexId(GradoopId id) {
    this.f1 = id;
  }

  public String getLabel() {
    return this.f2;
  }

  public void setLabel(String label) {
    this.f2 = label;
  }

}
