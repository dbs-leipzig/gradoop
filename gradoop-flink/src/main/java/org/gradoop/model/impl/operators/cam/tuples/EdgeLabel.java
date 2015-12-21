package org.gradoop.model.impl.operators.cam.tuples;

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.id.GradoopId;

/**
 * (graphId, sourceId, targetId, sourceLabel, edgeLabel, targetLabel)
 */
public class EdgeLabel extends Tuple6<GradoopId, GradoopId, GradoopId,
  String, String, String> {

  public EdgeLabel() {

  }

  public EdgeLabel(GradoopId graphId, GradoopId sourceId, GradoopId targetId,
    String edgeLabel) {

    this.f0 = graphId;
    this.f1 = sourceId;
    this.f2 = targetId;
    this.f3 = "";
    this.f4 = edgeLabel;
    this.f5 = "";
  }

  public void setSourceLabel(String sourceLabel) {
    this.f3 = sourceLabel;
  }

  public String getEdgeLabel() {
    return this.f4;
  }

  public void setEdgeLabel(String label) {
    this.f4 = label;
  }

  public void setTargetLabel(String targetLabel) {
    this.f5 = targetLabel;
  }

  public GradoopId getGraphId() {
    return this.f0;
  }

  public GradoopId getSourceId() {
    return this.f1;
  }

  public String getSourceLabel() { return this.f3; }

  public String getTargetLabel() { return this.f5; }

  public GradoopId getTargetId() {
    return this.f2;
  }

}
