package org.gradoop.flink.representation.common.elementdata;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

public class IdDirection extends Tuple2<GradoopId, Boolean> implements Comparable<IdDirection> {

  public IdDirection(GradoopId id, boolean outgoing) {
    super(id, outgoing);
  }

  public boolean isOutgoing() {
    return f1;
  }

  @Override
  public int compareTo(IdDirection that) {
    int comparison = this.f0.compareTo(that.f0);

    if (comparison == 0 && this.f1 != that.f1) {
      comparison = this.f1 ? -1 : 1;
    }

    return comparison;
  }

  public GradoopId getId() {
    return f0;
  }
}
