package org.gradoop.model.impl.operators.equality.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 23.11.15.
 */
public class EdgeDataAndTargetDataLabel extends
  Tuple3<GradoopId, String, String> {

  public EdgeDataAndTargetDataLabel(){
  }

  public EdgeDataAndTargetDataLabel(
    GradoopId sourceVertexId, String targetVertexLabel, String edgeLabel) {

    this.f0 = sourceVertexId;
    this.f1 = targetVertexLabel;
    this.f2 = edgeLabel;
  }

  public GradoopId getSourceVertexId() {
    return this.f0;
  }

  public void setSourceVertexId(GradoopId id) {
    this.f0 = id;
  }

  public String getTargetVertexLabel() {
    return this.f1;
  }

  public void setTargetVertexLabel(String label) {
    this.f1 = label;
  }

  public String getEdgeLabel() {
    return this.f2;
  }

  public void setEdgeLabel(String label) {
    this.f2 = label;
  }

}
