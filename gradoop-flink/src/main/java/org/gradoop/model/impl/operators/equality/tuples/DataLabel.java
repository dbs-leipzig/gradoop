package org.gradoop.model.impl.operators.equality.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMLabeled;
import org.gradoop.model.impl.id.GradoopId;

public class DataLabel extends Tuple2<GradoopId, String>
  implements EPGMLabeled{

  public DataLabel(){
  }

  public DataLabel(GradoopId id, String label) {
    this.f0 = id;
    this.f1 = label;
  }

  public GradoopId getId() {
    return this.f0;
  }

  public void setId(GradoopId id) {
    this.f0 = id;
  }

  public String getLabel() {
    return this.f1;
  }

  public void setLabel(String label) {
    this.f1 = label;
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
