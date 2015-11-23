package org.gradoop.model.impl.operators.equality.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMLabeled;
import org.gradoop.model.impl.id.GradoopId;

public class DataLabel extends Tuple3<GradoopId, GradoopId, String>
  implements EPGMLabeled{

  public DataLabel(){
  }

  public DataLabel(GradoopId id, String label) {
    this.f0 = new GradoopId();
    this.f1 = id;
    this.f2 = label;
  }

  public DataLabel(GradoopId graphId, GradoopId vertexId, String label) {
    this.f0 = graphId;
    this.f1 = vertexId;
    this.f2 = label;
  }

  public GradoopId getGraphId() {
    return this.f0;
  }

  public void setGraphId(GradoopId id) {
    this.f0 = id;
  }

  public GradoopId getId() {
    return this.f1;
  }

  public void setId(GradoopId id) {
    this.f1 = id;
  }

  public String getLabel() {
    return this.f2;
  }

  public void setLabel(String label) {
    this.f2 = label;
  }


  @Override
  public boolean equals(Object o) {
    boolean equals = o instanceof DataLabel;

    if(equals) {
      equals = this.getLabel().equals(((DataLabel) o).getLabel());
    }

    return equals;
  }

  @Override
  public int hashCode(){
    return this.getLabel().hashCode();
  }

}
